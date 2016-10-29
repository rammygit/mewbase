package com.tesco.mewbase.server.spi;

import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;

/**
 * Created by tim on 29/10/16.
 */
public interface ServerFactory {

    Server newServer(ServerOptions serverOptions);
}
