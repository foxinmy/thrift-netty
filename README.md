# thrift-netty

基于 Netty 实现的 Thrift RPC 通信组件

## 环境要求

* JDK 21+
## Maven

```xml
<dependency>
    <groupId>com.foxinmy</groupId>
    <artifactId>thrift-netty</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 使用

```java
ServerConf conf = new ServerConf(
        9083,
        Runtime.getRuntime().availableProcessors(),
        16 * 1024 * 1024,
        600
);

MyService.Iface handler = new MyServiceHandler();

MyService.Processor<MyService.Iface> processor =
        new MyService.Processor<>(handler);

TServer server = new TServer(
        conf,
        handler,
        processor.getProcessMapView()
);

server.start();
```

##  配置

| 参数                | 说明                 |
|-------------------|--------------------|
| `bindPort`        | 监听端口             |
| `handleThreads`   | 工作线程数            |
| `maxFrameSize`    | Thrift 最大 Frame 大小 |
| `readIdleTimeout` | 读空闲超时时间，单位：秒  |

## License

Apache License 2.0