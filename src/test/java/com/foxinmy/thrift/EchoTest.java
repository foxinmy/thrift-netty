package com.foxinmy.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EchoTest {
    private final static int PORT = 9083;
    private static TServer server;
    private static TTransport transport;

    @AfterAll
    static void release() {
        if (server != null) server.stop();
        if (transport != null) transport.close();
    }

    @Test
    void server() throws Exception {
        ServerConf conf = new ServerConf(PORT);
        EchoService.Iface handler = message -> "echo: " + message;
        EchoService.Processor<EchoService.Iface> processor = new EchoService.Processor<>(handler);
        server = new TServer(conf, handler, processor.getProcessMapView());
        server.start();
    }

    @Test
    void client() throws Exception {
        transport = new TFramedTransport(new TSocket("127.0.0.1", PORT));
        EchoService.Client client = new EchoService.Client(new TBinaryProtocol(transport));
        transport.open();
        assertEquals("echo: hello", client.echo("hello"));
    }
}
