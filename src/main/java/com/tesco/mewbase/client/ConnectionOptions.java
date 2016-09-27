package com.tesco.mewbase.client;

/**
 * Created by tim on 22/09/16.
 */
public class ConnectionOptions {

    private String host = "localhost";
    private int port = 7451;

    public String getHost() {
        return host;
    }

    public ConnectionOptions setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ConnectionOptions setPort(int port) {
        this.port = port;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionOptions that = (ConnectionOptions) o;

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
        return "ConnectionOptions{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
