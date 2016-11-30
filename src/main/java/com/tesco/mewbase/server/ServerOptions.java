package com.tesco.mewbase.server;

import com.tesco.mewbase.auth.impl.DummyAuthProvider;
import com.tesco.mewbase.auth.MewbaseAuthProvider;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import io.vertx.core.net.NetServerOptions;

import java.util.Arrays;

/**
 * Created by tim on 22/09/16.
 */
public class ServerOptions {

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 7451;
    public static final String DEFAULT_DOCS_DIR = "docs";

    private String[] channels;
    private FileLogManagerOptions fileLogManagerOptions = new FileLogManagerOptions();
    private NetServerOptions netServerOptions = new NetServerOptions().setPort(DEFAULT_PORT).setHost(DEFAULT_HOST);
    private String docsDir = DEFAULT_DOCS_DIR;
    private String[] binders;
    private MewbaseAuthProvider authProvider = new DummyAuthProvider();

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

    public String getDocsDir() {
        return docsDir;
    }

    public ServerOptions setDocsDir(String docsDir) {
        this.docsDir = docsDir;
        return this;
    }

    public String[] getBinders() {
        return binders;
    }

    public ServerOptions setBinders(String[] binders) {
        this.binders = binders;
        return this;
    }

    public MewbaseAuthProvider getAuthProvider() {
        return authProvider;
    }

    public ServerOptions setAuthProvider(MewbaseAuthProvider authProvider) {
        this.authProvider = authProvider;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerOptions that = (ServerOptions)o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(channels, that.channels)) return false;
        if (fileLogManagerOptions != null ? !fileLogManagerOptions.equals(that.fileLogManagerOptions) : that.fileLogManagerOptions != null)
            return false;
        if (netServerOptions != null ? !netServerOptions.equals(that.netServerOptions) : that.netServerOptions != null)
            return false;
        if (docsDir != null ? !docsDir.equals(that.docsDir) : that.docsDir != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(binders, that.binders)) return false;
        return authProvider != null ? authProvider.equals(that.authProvider) : that.authProvider == null;

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(channels);
        result = 31 * result + (fileLogManagerOptions != null ? fileLogManagerOptions.hashCode() : 0);
        result = 31 * result + (netServerOptions != null ? netServerOptions.hashCode() : 0);
        result = 31 * result + (docsDir != null ? docsDir.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(binders);
        result = 31 * result + (authProvider != null ? authProvider.hashCode() : 0);
        return result;
    }
}
