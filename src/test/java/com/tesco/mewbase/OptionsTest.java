package com.tesco.mewbase;

import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class OptionsTest extends MewbaseTestBase {

    private final static Logger log = LoggerFactory.getLogger(OptionsTest.class);
    private static final int fsize = 4*1024;

    @Test
    public void testClientOptions() throws Exception {

        ClientOptions options = new ClientOptions();
        assertEquals(new NetClientOptions(), options.getNetClientOptions());
        assertEquals(ClientOptions.DEFAULT_PORT, options.getPort());
        assertEquals(ClientOptions.DEFAULT_HOST, options.getHost());

        NetClientOptions netClientOptions2 = new NetClientOptions();
        options.setNetClientOptions(netClientOptions2);
        assertSame(netClientOptions2, options.getNetClientOptions());

        options.setPort(1547);
        assertEquals(1547, options.getPort());

        options.setHost("somehost");
        assertEquals("somehost", options.getHost());
    }

    @Test
    public void testServerOptions() throws Exception {

        ServerOptions options = new ServerOptions();
        assertEquals(new NetServerOptions().setPort(ServerOptions.DEFAULT_PORT), options.getNetServerOptions());
        NetServerOptions netServerOptions2 = new NetServerOptions();
        options.setNetServerOptions(netServerOptions2);
        assertSame(netServerOptions2, options.getNetServerOptions());

        options.setDocsDir("foo");
        assertSame("foo", options.getDocsDir());

        String[] channels = new String[]{"foo", "bar"};
        assertNull(options.getChannels());
        options.setChannels(channels);
        assertSame(channels, options.getChannels());

        String[] binders = new String[]{"foo", "bar"};
        assertNull(options.getBinders());
        options.setBinders(binders);
        assertSame(binders, options.getBinders());

        options.setDocsDir("foo");
        assertSame("foo", options.getDocsDir());

        options.setMaxLogChunkSize(fsize);
        options.setMaxRecordSize(fsize);
        options.setPreallocateSize(fsize);
        options.setMaxRecordSize(fsize);

        assert(options.getMaxLogChunkSize() == fsize);
        assert(options.getPreallocateSize() == fsize);
        assert(options.getMaxRecordSize() == fsize);
        assert(options.getReadBufferSize() == fsize);
    }
}
