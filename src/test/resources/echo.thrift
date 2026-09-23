namespace java com.foxinmy.thrift

service EchoService {
    string echo(1: string message)
}