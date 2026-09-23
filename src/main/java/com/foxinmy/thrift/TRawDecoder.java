package com.foxinmy.thrift;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransportException;

import java.util.List;

@Slf4j
public class TRawDecoder extends ReplayingDecoder<TRawDecoder.State> {
    private static final int VERSION_MASK = 0xffff0000;
    private static final int VERSION_1 = 0x80010000;
    private final int maxFrameSize;

    enum State {READ_MESSAGE, READ_STRUCT_FIELDS}

    public TRawDecoder(int maxFrameSize) {
        super(State.READ_MESSAGE);
        this.maxFrameSize = maxFrameSize;
    }


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()) {
            case READ_MESSAGE:
                in.markReaderIndex();
                // int size = readI32()
                // new TMessage(readString(), (byte)(size & 0x000000ff), readI32())
                int size = in.readInt();
                int version = size & VERSION_MASK;
                if (version != VERSION_1)
                    throw new TProtocolException(TProtocolException.BAD_VERSION, "Bad version in readMessage");
                int len = in.readInt();
                in.skipBytes(len);
                in.skipBytes(4);
                checkpoint(State.READ_STRUCT_FIELDS);
            case READ_STRUCT_FIELDS:
                while (true) {
                    // new TField("", readByte(), readI16())
                    byte type = in.readByte();
                    if (type == TType.STOP) break;
                    in.skipBytes(2);
                    skipTProtocol(in, type, 0);
                }
                checkpoint(State.READ_MESSAGE);
                int endIndex = in.readerIndex();
                int startIndex = in.resetReaderIndex().readerIndex();
                int messageBytes = endIndex - startIndex;
                if (messageBytes > maxFrameSize) {
                    ctx.fireExceptionCaught(new TTransportException(TTransportException.MESSAGE_SIZE_LIMIT, "Message size exceeds limit: " + maxFrameSize));
                    return;
                }
                out.add(in.readerIndex(endIndex).retainedSlice(startIndex, messageBytes));
                break;
            default:
                throw new Error("Unknown State");
        }
    }

    protected void skipTProtocol(ByteBuf buf, byte type, int depth) throws TException {
        if (depth == Integer.MAX_VALUE) throw new TException("Maximum skip depth exceeded");
        switch (type) {
            case TType.BOOL, TType.BYTE:
                buf.skipBytes(1);
                break;

            case TType.I16:
                buf.skipBytes(2);
                break;

            case TType.I32:
                buf.skipBytes(4);
                break;

            case TType.I64, TType.DOUBLE:
                buf.skipBytes(8);
                break;

            case TType.UUID:
                buf.skipBytes(16);
                break;

            case TType.STRING:
                int len = buf.readInt();
                checkMessageReadLength(len);
                buf.skipBytes(len);
                break;

            case TType.STRUCT:
                while (true) {
                    // new TField("", readByte(), readI16())
                    byte fieldType = buf.readByte();
                    if (fieldType == TType.STOP) break;
                    buf.skipBytes(2);
                    skipTProtocol(buf, fieldType, depth + 1);
                }
                break;

            case TType.MAP:
                // new TMap(readByte(), readByte(), readI32())
                byte keyType = buf.readByte();
                byte valueType = buf.readByte();
                int mapSize = buf.readInt();
                checkContainerReadLength(mapSize);
                for (int i = 0; i < mapSize; i++) {
                    skipTProtocol(buf, keyType, depth + 1);
                    skipTProtocol(buf, valueType, depth + 1);
                }
                break;

            case TType.SET:
                // new TSet(readByte(), readI32())
                byte setType = buf.readByte();
                int setSize = buf.readInt();
                checkContainerReadLength(setSize);
                for (int i = 0; i < setSize; i++) {
                    skipTProtocol(buf, setType, depth + 1);
                }
                break;

            case TType.LIST:
                // new TList(readByte(), readI32())
                byte listType = buf.readByte();
                int listSize = buf.readInt();
                checkContainerReadLength(listSize);
                for (int i = 0; i < listSize; i++) {
                    skipTProtocol(buf, listType, depth + 1);
                }
                break;

            default:
                throw new TProtocolException(TProtocolException.INVALID_DATA, "Unrecognized type " + type);
        }
    }

    private void checkMessageReadLength(int length) throws TException {
        if (length < 0) {
            throw new TProtocolException(TProtocolException.NEGATIVE_SIZE, "Negative length: " + length);
        }
        if (TConfiguration.DEFAULT_MAX_MESSAGE_SIZE < length)
            throw new TTransportException(
                    TTransportException.MESSAGE_SIZE_LIMIT,
                    "Message size exceeds limit: " + TConfiguration.DEFAULT_MAX_MESSAGE_SIZE);
    }

    private void checkContainerReadLength(int length) throws TProtocolException {
        if (length < 0) {
            throw new TProtocolException(TProtocolException.NEGATIVE_SIZE, "Negative length: " + length);
        }
    }
}
