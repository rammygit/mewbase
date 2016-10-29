package com.tesco.mewbase.shopping;

import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.ServerImpl;

/**
 * Created by tim on 30/09/16.
 */
public class ExampleServer {

    public static void main(String[] args) {

        Server mewbase = Server.newServer(new ServerOptions());
        mewbase.installFunction("myfunc1", new SubDescriptor().setChannel("com.tesco.foo"), (ctx, re) -> {
            //do something
        });
        mewbase.installFunction("myfunc1", new SubDescriptor().setChannel("com.tesco.bar"), (ctx, re) -> {
            //do something
        });
        mewbase.start();
    }
}
