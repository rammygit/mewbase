package com.tesco.mewbase.server;

import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import io.vertx.core.net.NetServerOptions;

/**
 * Created by tim on 22/09/16.
 */
public class ServerOptions {

    private String host = "0.0.0.0";
    private int port = 7451;
    private String[] channels;
    private FileLogManagerOptions fileLogManagerOptions;
    private NetServerOptions netServerOptions;

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

    public String[] getChannels() {
        return channels;
    }

    public ServerOptions setChannels(String[] channels) {
        this.channels = channels;
        return this;
    }

    public FileLogManagerOptions getFileLogManagerOptions() {
        return fileLogManagerOptions;
    }

    public ServerOptions setFileLogManagerOptions(FileLogManagerOptions fileLogManagerOptions) {
        this.fileLogManagerOptions = fileLogManagerOptions;
        return this;
    }

    public NetServerOptions getNetServerOptions() {
        return netServerOptions;
    }

    public ServerOptions setNetServerOptions(NetServerOptions netServerOptions) {
        this.netServerOptions = netServerOptions;
        return this;
    }
}
