package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;
import io.vertx.core.net.NetClientOptions;

/**
 * Created by tim on 22/09/16.
 */
public class ClientOptions {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 7451;

    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private NetClientOptions netClientOptions = new NetClientOptions();
    private BsonObject authInfo;

    public String getHost() {
        return host;
    }

    public ClientOptions setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ClientOptions setPort(int port) {
        this.port = port;
        return this;
    }

    public NetClientOptions getNetClientOptions() {
        return netClientOptions;
    }

    public ClientOptions setNetClientOptions(NetClientOptions netClientOptions) {
        this.netClientOptions = netClientOptions;
        return this;
    }


    public BsonObject getAuthInfo() {
        return authInfo;
    }

    public ClientOptions setAuthInfo(BsonObject authInfo) {
        this.authInfo = authInfo;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientOptions that = (ClientOptions)o;

        if (port != that.port) return false;
        if (netClientOptions != null && !netClientOptions.equals(that.getNetClientOptions())) return false;

        return host != null ? host.equals(that.host) : that.host == null;

    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "ClientOptions{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

}
