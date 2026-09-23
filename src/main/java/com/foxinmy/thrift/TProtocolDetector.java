package com.foxinmy.thrift;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.List;

public class TProtocolDetector extends ByteToMessageDecoder {
    private final int maxFrameSize;

    public TProtocolDetector(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 4) return;
        ChannelPipeline pipeline = ctx.pipeline();
        short magic = in.getUnsignedByte(0);
        if (magic < 0x80) {
            pipeline.addAfter(ctx.name(), "lengthFieldPrepender", new LengthFieldPrepender(4));
            pipeline.addAfter(ctx.name(), "lengthFieldBasedFrameDecoder", new LengthFieldBasedFrameDecoder
                    (maxFrameSize, 0, 4, 0, 4));
        } else {
            pipeline.addAfter(ctx.name(), "thriftRawDecoder", new TRawDecoder(maxFrameSize));
        }
        pipeline.remove(this);
    }
}
