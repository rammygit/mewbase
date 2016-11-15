package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.bson.BsonPath;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class OptionsTest extends MewbaseTestBase {

    private final static Logger log = LoggerFactory.getLogger(OptionsTest.class);

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
        assertEquals(new FileLogManagerOptions(), options.getFileLogManagerOptions());

        NetServerOptions netServerOptions2 = new NetServerOptions();
        options.setNetServerOptions(netServerOptions2);
        assertSame(netServerOptions2, options.getNetServerOptions());

        FileLogManagerOptions fileLogManagerOptions2 = new FileLogManagerOptions();
        options.setFileLogManagerOptions(fileLogManagerOptions2);
        assertSame(fileLogManagerOptions2, options.getFileLogManagerOptions());

        String[] channels = new String[] {"foo", "bar"};
        assertNull(options.getChannels());
        options.setChannels(channels);
        assertSame(channels, channels);
    }

}
