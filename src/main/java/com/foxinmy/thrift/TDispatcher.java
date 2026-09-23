package com.foxinmy.thrift;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.TSerializable;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.foxinmy.thrift.Utils.*;

@Slf4j
@ChannelHandler.Sharable
public class TDispatcher extends ChannelInboundHandlerAdapter {
    private static final Pattern BAD_CONNECTION_MESSAGE = Pattern.compile(
            "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$", Pattern.CASE_INSENSITIVE);
    private static final Set<Class<? extends Exception>> BAD_CONNECTION_EXCEPTION = Set.of(TTransportException.class
            , TooLongFrameException.class);
    private static final TMessage DEFAULT_TMESSAGE = new TMessage();
    private final Object handler;
    private final Map<String, ? extends ProcessFunction> functions;
    private final ExecutorService executor;
    private final int maxFrameSize;

    TDispatcher(Object handler, Map<String, ? extends ProcessFunction> functions, ExecutorService executor, int maxFrameSize) {
        this.handler = handler;
        this.functions = functions;
        this.executor = executor;
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.debug("{} connected", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.debug("disconnect {}", ctx.channel().remoteAddress());
    }

    protected void writeRespond(long requestTime, ChannelHandlerContext ctx, TMessage message, TSerializable respond, String diagnostics) {
        ByteBuf res = ctx.alloc().buffer();
        String remoteAddress = ctx.channel().remoteAddress().toString();
        int messageId = message.getSeqid();
        try {
            TProtocol protocol = new TBufferProtocol(res, maxFrameSize);
            protocol.writeMessageBegin(message);
            respond.write(protocol);
            protocol.writeMessageEnd();
            int bytes = res.readableBytes();
            String messageResult = abbreviateWithLength(respond.toString().substring(message.name.length() + 1), 512);
            ctx.writeAndFlush(res).addListener((ChannelFutureListener) future -> {
                if (message.type == TMessageType.EXCEPTION) {
                    if (message.equals(DEFAULT_TMESSAGE)) {
                        log.error("{} Failed read message\r\n  {}", remoteAddress, diagnostics);
                    } else {
                        log.error("{} <TMessage {}> processed\r\n  {}", remoteAddress, messageId, diagnostics);
                    }
                } else {
                    long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requestTime);
                    if (durationMillis >= 1000) {
                        log.warn("{} <TMessage {}> processed - {}, {} bytes\r\n  {}", remoteAddress, messageId, bytes
                                , formatDuration(durationMillis), messageResult);
                    } else {
                        log.info("{} <TMessage {}> processed - {} bytes\r\n  {}", remoteAddress, messageId, bytes, messageResult);
                    }
                }
            });
        } catch (TException e) {
            ReferenceCountUtil.release(res);
            log.error("Failed write <TMessage {}>: {}", message.getSeqid(), getExceptionMessage(e));
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf req = ((ByteBuf) msg);
        long requestTime = System.nanoTime();
        TMessage message = DEFAULT_TMESSAGE;
        int bytes = req.readableBytes();
        try {
            TProtocol protocol = new TBufferProtocol(req, maxFrameSize);
            message = protocol.readMessageBegin();
            String messageName = message.getName();
            ProcessFunction func = functions.get(messageName);
            TBase args;
            if (func == null) {
                TProtocolUtil.skip(protocol, TType.STRUCT);
                protocol.readMessageEnd();
                throw new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + messageName + "'");
            } else {
                args = func.getEmptyArgsInstance();
                args.read(protocol);
                protocol.readMessageEnd();
            }
            String remoteAddress = ctx.channel().remoteAddress().toString();
            int messageId = message.getSeqid();
            String messageArgs = abbreviateWithLength(args.toString().substring(messageName.length() + 1), 512);
            int queued = ((ThreadPoolExecutor) executor).getQueue().size();
            if (queued > 0) {
                log.warn("{} <TMessage {} '{}'> - {} bytes, {} queued\r\n  {}", remoteAddress, messageId, messageName, bytes, queued, messageArgs);
            } else {
                log.info("{} <TMessage {} '{}'> - {} bytes\r\n  {}", remoteAddress, messageId, messageName, bytes, messageArgs);
            }
            final TMessage m = message;
            executor.execute(() -> {
                try {
                    TBase result = func.getResult(handler, args);
                    if (func.isOneway()) return;
                    writeRespond(requestTime, ctx, new TMessage(messageName, TMessageType.REPLY, messageId), result, null);
                } catch (Exception e) {
                    flushException(requestTime, ctx, e, m);
                }
            });
        } catch (TException e) {
            flushException(requestTime, ctx, e, message);
        } catch (RejectedExecutionException e) { // never happen
            flushException(requestTime, ctx, new TApplicationException(TApplicationException.INTERNAL_ERROR, "Request exceeds limit")
                    , message);
        } finally {
            ReferenceCountUtil.release(req);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent e) {
            if (e.state() == IdleState.READER_IDLE) {
                ctx.close();
                log.warn("Closing idle connection: {}", ctx.channel().remoteAddress());
            }
        }
    }

    private void flushException(long requestTime, ChannelHandlerContext ctx, Throwable cause, TMessage message) {
        List<Throwable> exceptions = getThrowableList(cause);
        Throwable rootCause = exceptions.isEmpty() ? cause : exceptions.getLast();
        if (closeBadConnection(ctx, rootCause)) return;
        String traceMessage = exceptions.stream().map(e -> {
            StackTraceElement[] traceElements = e.getStackTrace();
            int maxTrace = Math.min(traceElements.length, 3);
            StringBuilder traceBuilder = new StringBuilder();
            for (int i = 0; i < maxTrace; i++) {
                traceBuilder.append("\r\n\t").append(traceElements[i].toString());
            }
            return String.format("%s%s", getExceptionMessage(e), traceBuilder);
        }).collect(Collectors.joining("\r\n  "));
        int messageId = message.getSeqid();
        String messageName = message.getName();
        TApplicationException exception = rootCause instanceof TApplicationException ?
                (TApplicationException) rootCause : new TApplicationException(rootCause instanceof TException ?
                TApplicationException.PROTOCOL_ERROR : TApplicationException.INTERNAL_ERROR, cause.getMessage());
        writeRespond(requestTime, ctx, new TMessage(messageName, TMessageType.EXCEPTION, messageId), exception, traceMessage);
    }

    private boolean closeBadConnection(ChannelHandlerContext ctx, Throwable cause) {
        if (!ctx.channel().isActive()) return false;
        boolean badConnection;
        Class<?> clazz = cause.getClass();
        String message = cause.getMessage();
        if (BAD_CONNECTION_EXCEPTION.contains(clazz)) {
            badConnection = true;
        } else {
            // first try to match connection reset / broke peer based on the regex.
            // Close the connection explicitly just in case the transport
            // did not close the connection automatically.
            badConnection = message != null && BAD_CONNECTION_MESSAGE.matcher(message).matches();
        }
        if (badConnection) {
            ctx.close();
            log.warn("Closing bad connection: {} cause by {}: {}", ctx.channel().remoteAddress(), clazz.getName(), message);
        }
        return badConnection;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // tcp 健康探测
        if (cause instanceof java.net.SocketException
            && cause.getMessage() != null
            && cause.getMessage().contains("Connection reset")) {
            ctx.close();
            return;
        }
        flushException(System.nanoTime(), ctx, cause, DEFAULT_TMESSAGE);
    }
}
