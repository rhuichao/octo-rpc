package com.meituan.dorado.testserver;

import com.meituan.dorado.bootstrap.ServiceBootstrap;
import com.meituan.dorado.test.thrift.api.Echo;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class OriginThriftClient {
    public static void main(String[] args) {

        TTransport transport = null;
        try {
            transport = new TFramedTransport(new TSocket("localhost", 9001));
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            Echo.Iface client = new Echo.Client(protocol);
            String echo = client.echo("abc");
            System.out.println(echo);

             echo = client.echo("11111");
            System.out.println(echo);

             echo = client.echo("22222");
            System.out.println(echo);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            transport.close();
          //  ServiceBootstrap.clearGlobalResource();
        }

    }
}
