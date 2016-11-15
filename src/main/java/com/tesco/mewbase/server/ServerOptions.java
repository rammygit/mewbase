package com.tesco.mewbase.server;

import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import io.vertx.core.net.NetServerOptions;

import java.util.Arrays;

/**
 * Created by tim on 22/09/16.
 */
public class ServerOptions {

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 7451;

    private String[] channels;
    private FileLogManagerOptions fileLogManagerOptions = new FileLogManagerOptions();
    private NetServerOptions netServerOptions = new NetServerOptions().setPort(DEFAULT_PORT).setHost(DEFAULT_HOST);

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerOptions that = (ServerOptions) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(channels, that.channels)) return false;
        if (fileLogManagerOptions != null ? !fileLogManagerOptions.equals(that.fileLogManagerOptions) : that.fileLogManagerOptions != null)
            return false;
        return netServerOptions != null ? netServerOptions.equals(that.netServerOptions) : that.netServerOptions == null;

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(channels);
        result = 31 * result + (fileLogManagerOptions != null ? fileLogManagerOptions.hashCode() : 0);
        result = 31 * result + (netServerOptions != null ? netServerOptions.hashCode() : 0);
        return result;
    }
}
