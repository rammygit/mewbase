package com.tesco.mubase.server;

/**
 * Created by tim on 22/09/16.
 */
public class ServerOptions {

    private String host = "0.0.0.0";
    private int port = 7451;

    public String getHost() {
        return host;
    }

    public ServerOptions setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ServerOptions setPort(int port) {
        this.port = port;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerOptions that = (ServerOptions) o;

        if (port != that.port) return false;
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
        return "ServerOptions{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
