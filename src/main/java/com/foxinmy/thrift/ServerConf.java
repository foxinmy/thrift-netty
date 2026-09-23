package com.foxinmy.thrift;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ServerConf {
    private int port;
    private int threads;
    private int maxFrameSize;
    private int readIdleTimeout;
}
