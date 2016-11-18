package com.tesco.mewbase;

import com.tesco.mewbase.auth.MewbaseVertxAuthProvider;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.ClientDelivery;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.auth.shiro.PropertiesProviderConstants;
import io.vertx.ext.auth.shiro.ShiroAuth;
import io.vertx.ext.auth.shiro.ShiroAuthOptions;
import io.vertx.ext.auth.shiro.ShiroAuthRealmType;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.function.Consumer;

@RunWith(VertxUnitRunner.class)
public class ShiroPropertiesAuthenticationTest extends ServerTestBase {

    @Override
    protected ServerOptions createServerOptions(File logDir) {
        FileLogManagerOptions fileLogManagerOptions = new FileLogManagerOptions().setLogDir(logDir.getPath());

        return new ServerOptions().setChannels(new String[]{TEST_CHANNEL_1, TEST_CHANNEL_2})
                .setFileLogManagerOptions(fileLogManagerOptions)
                .setAuthProvider(new MewbaseVertxAuthProvider(createShiroAuthProvider()));
    }

    @Override
    protected ClientOptions createClientOptions() {
        BsonObject authInfo = new BsonObject().put("username", "mew").put("password", "base");
        return new ClientOptions().setNetClientOptions(new NetClientOptions()).setAuthInfo(authInfo);
    }

    private ShiroAuth createShiroAuthProvider() {
        JsonObject config = new JsonObject();
        config.put(PropertiesProviderConstants.PROPERTIES_PROPS_PATH_FIELD, "classpath:test-shiro-auth.properties");

        ShiroAuthOptions shiroAuthOptions = new ShiroAuthOptions().setType(ShiroAuthRealmType.PROPERTIES).setConfig(config);

        return ShiroAuth.create(vertx, shiroAuthOptions);
    }

    @Test
    public void testAuthentication(TestContext context) throws Exception {
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        Async async = context.async();

        BsonObject sent = new BsonObject().put("foo", "bar");

        Consumer<ClientDelivery> handler = re -> async.complete();

        client.subscribe(descriptor, handler).get();

        prod.publish(sent).get();
    }

}
