package com.foxinmy.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EchoTest {
    private final static int PORT = 9083;

    @Test
    void test() throws Exception {
        ServerConf conf = new ServerConf(PORT, Runtime.getRuntime().availableProcessors(), 16 * 1024 * 1024, 300);
        EchoService.Iface handler = message -> "echo: " + message;
        EchoService.Processor<EchoService.Iface> processor = new EchoService.Processor<>(handler);
        TServer server = new TServer(conf, handler, processor.getProcessMapView());
        server.start(false);
        waitForServer();
        try {
            TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", PORT));
            try {
                EchoService.Client client = new EchoService.Client(new TBinaryProtocol(transport));
                transport.open();
                assertEquals("echo: hello", client.echo("hello"));
            } finally {
                transport.close();
            }
        } finally {
            server.stop(true);
        }
    }

    private void waitForServer() throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", PORT), 100);
                return;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        throw new IllegalStateException("Thrift server did not start within 5 seconds");
    }
}
