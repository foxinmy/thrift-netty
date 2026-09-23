package com.foxinmy.thrift;

import static org.apache.thrift.TConfiguration.DEFAULT_MAX_FRAME_SIZE;

public class ServerConf {
    private final int bindPort;
    private final int handleThreads;
    private final int maxFrameSize;
    private final int readIdleTimeout;

    public ServerConf(int bindPort) {
        this(bindPort, Runtime.getRuntime().availableProcessors(), DEFAULT_MAX_FRAME_SIZE, 600);
    }

    public ServerConf(int bindPort, int handleThreads, int maxFrameSize, int readIdleTimeout) {
        this.bindPort = bindPort;
        this.handleThreads = handleThreads;
        this.maxFrameSize = maxFrameSize;
        this.readIdleTimeout = readIdleTimeout;
    }

    public int getBindPort() {
        return bindPort;
    }

    public int getHandleThreads() {
        return handleThreads;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public int getReadIdleTimeout() {
        return readIdleTimeout;
    }

    @Override
    public String toString() {
        return "ServerConf{" +
               "bindPort=" + bindPort +
               ", handleThreads=" + handleThreads +
               ", maxFrameSize=" + maxFrameSize +
               ", readIdleTimeout=" + readIdleTimeout +
               '}';
    }
}
