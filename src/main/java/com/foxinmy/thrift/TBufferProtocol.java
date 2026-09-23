package com.foxinmy.thrift;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.apache.thrift.TConfiguration.DEFAULT_MAX_MESSAGE_SIZE;
import static org.apache.thrift.TConfiguration.DEFAULT_RECURSION_DEPTH;

@Slf4j
public class TBufferProtocol extends TBinaryProtocol {
    private final ByteBuf byteBuf;

    public TBufferProtocol(ByteBuf byteBuf, int maxFrameSize) {
        super(new TEmptyTransport(maxFrameSize));
        this.byteBuf = byteBuf;
    }

    @Override
    public boolean readBool() {
        return byteBuf.readBoolean();
    }

    @Override
    public byte readByte() {
        return byteBuf.readByte();
    }

    @Override
    public short readI16() {
        return byteBuf.readShort();
    }

    @Override
    public int readI32() {
        return byteBuf.readInt();
    }

    @Override
    public long readI64() {
        return byteBuf.readLong();
    }

    @Override
    public UUID readUuid() {
        long lsb = byteBuf.readLong();
        long msb = byteBuf.readLong();
        return new UUID(msb, lsb);
    }

    @Override
    public String readStringBody(int length) throws TException {
        checkReadLength(length);
        return byteBuf.readString(length, StandardCharsets.UTF_8);
    }

    @Override
    public double readDouble() {
        return byteBuf.readDouble();
    }

    @Override
    public String readString() throws TException {
        int length = byteBuf.readInt();
        return readStringBody(length);
    }

    @Override
    public ByteBuffer readBinary() throws TException {
        int length = byteBuf.readInt();
        checkReadLength(length);
        return byteBuf.readBytes(length).nioBuffer();
    }

    private void checkReadLength(int length) throws TException {
        if (length < 0) throw new TProtocolException(TProtocolException.NEGATIVE_SIZE, "Negative length: " + length);
        getTransport().checkReadBytesAvailable(length);
    }

    @Override
    protected void skipBytes(int numBytes) {
        byteBuf.skipBytes(numBytes);
    }

    @Override
    public void writeBool(boolean b) {
        byteBuf.writeBoolean(b);
    }

    @Override
    public void writeByte(byte b) {
        byteBuf.writeByte(b);
    }

    @Override
    public void writeI16(short i16) {
        byteBuf.writeShort(i16);
    }

    @Override
    public void writeI32(int i32) {
        byteBuf.writeInt(i32);
    }

    @Override
    public void writeI64(long i64) {
        byteBuf.writeLong(i64);
    }

    @Override
    public void writeUuid(UUID uuid) {
        long lsb = uuid.getLeastSignificantBits();
        long msb = uuid.getMostSignificantBits();
        byteBuf.writeLong(lsb);
        byteBuf.writeLong(msb);
    }

    @Override
    public void writeDouble(double dub) {
        byteBuf.writeDouble(dub);
    }

    @Override
    public void writeString(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
    }

    @Override
    public void writeBinary(ByteBuffer buf) {
        byteBuf.writeBytes(buf);
    }

    static class TEmptyTransport extends TTransport {
        private final TConfiguration conf;

        TEmptyTransport(int maxFrameSize) {
            this.conf = new TConfiguration(DEFAULT_MAX_MESSAGE_SIZE, maxFrameSize, DEFAULT_RECURSION_DEPTH);
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void open() {

        }

        @Override
        public void close() {

        }

        @Override
        public int read(byte[] buf, int off, int len) {
            throw new IllegalCallerException();
        }

        @Override
        public void write(byte[] buf, int off, int len) {
            throw new IllegalCallerException();
        }

        @Override
        public TConfiguration getConfiguration() {
            return conf;
        }

        @Override
        public void updateKnownMessageSize(long size) {
            throw new IllegalCallerException();
        }

        @Override
        public void checkReadBytesAvailable(long numBytes) throws TTransportException {
            int maxMessageSize = conf.getMaxMessageSize();
            if (maxMessageSize < numBytes)
                throw new TTransportException(
                        TTransportException.MESSAGE_SIZE_LIMIT,
                        "Message size exceeds limit: " + maxMessageSize);
        }
    }
}
