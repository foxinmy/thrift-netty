package com.foxinmy.thrift;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.bootstrap.ServerBootstrapConfig;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.ProcessFunction;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

@Slf4j
public class TServer {
    private final ServerConf conf;
    private final Object handler;
    private final Map<String, ? extends ProcessFunction> functions;
    private ExecutorService executor;
    private ServerBootstrap bootstrap;
    private Channel channel;

    public TServer(ServerConf conf, Object handler, Map<String, ? extends ProcessFunction> functions) {
        this.conf = conf;
        this.handler = handler;
        this.functions = functions;
    }

    private ServerBootstrap newBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        boolean epollAvailable = Epoll.isAvailable();
        IoHandlerFactory ioHandlerFactory = epollAvailable ? EpollIoHandler.newFactory() : NioIoHandler.newFactory();
        bootstrap.group(new MultiThreadIoEventLoopGroup(1, new DefaultThreadFactory("acceptor"), ioHandlerFactory),
                new MultiThreadIoEventLoopGroup(Runtime.getRuntime().availableProcessors() + 1
                        , new DefaultThreadFactory("worker"), ioHandlerFactory));
        if (epollAvailable) {
            bootstrap.channel(EpollServerSocketChannel.class);
        } else {
            bootstrap.channel(NioServerSocketChannel.class);
        }
        return bootstrap;
    }

    public void start(boolean blocking) throws InterruptedException {
        bootstrap = newBootstrap().option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_RCVBUF, 128 * 1024)
                .childOption(ChannelOption.SO_SNDBUF, 256 * 1024)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        final int maxFrameSize = conf.getMaxFrameSize();
        executor = new ThreadPoolExecutor(
                conf.getThreads(), conf.getThreads() * 2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10240),
                new DefaultThreadFactory("exec"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        final TDispatcher dispatcher = new TDispatcher(handler, functions, executor, maxFrameSize);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new IdleStateHandler(conf.getReadIdleTimeout(), 0, 0, TimeUnit.SECONDS));
                pipeline.addLast(new TProtocolDetector(maxFrameSize));
                pipeline.addLast(dispatcher);
            }
        });
        int port = conf.getPort();
        ChannelFuture future = bootstrap.bind("0.0.0.0", port).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                channel = f.channel();
                log.info("RPC Server {} startup successfully", channel);
            } else {
                throw new RuntimeException("RPC Server :" + port + " startup failed", f.cause());
            }
        });
        if (blocking) future.sync().channel().closeFuture().sync();
    }

    public void stop(boolean blocking) {
        ChannelFuture future = channel != null ? channel.close().addListener((ChannelFutureListener) f -> log.info("TServer channel shutdown: {}", f.channel())) : null;
        if (blocking && future != null) future.syncUninterruptibly();

        if (bootstrap != null) {
            ServerBootstrapConfig config = bootstrap.config();
            io.netty.util.concurrent.Future<?> shutdownFuture = config.group().shutdownGracefully()
                    .addListener(f -> log.info("TServer acceptor shutdown: {}", Objects.toString(f.cause(), "OK")));
            if (blocking) shutdownFuture.syncUninterruptibly();
            shutdownFuture = config.childGroup().shutdownGracefully()
                    .addListener(f -> log.info("TServer worker shutdown: {}", Objects.toString(f.cause(), "OK")));
            if (blocking) shutdownFuture.syncUninterruptibly();
        }
        if (executor != null) executor.shutdown();
    }
}
