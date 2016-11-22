package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.server.impl.Codec;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;


/**
 *
 * This class serves as an adapter for the {@link io.vertx.ext.auth.AuthProvider}
 *
 * It will container a {@link io.vertx.ext.auth.AuthProvider} attribute, which is then used
 * in the authenticate method.
 *
 * For the authentication information, the attributes just need to be inserted in the BsonObject.
 *
 * The mapping to {@link io.vertx.core.json.JsonObject} is done inside the authenticate method.
 *
 */
public class MewbaseVertxAuthProvider implements MewbaseAuthProvider {

    private final static Logger log = LoggerFactory.getLogger(MewbaseVertxAuthProvider.class);

    private AuthProvider authProvider;

    public MewbaseVertxAuthProvider(AuthProvider authProvider) {
        this.authProvider = authProvider;
    }

    @Override
    public void authenticate(BsonObject authInfo, Consumer<BsonObject> consumer) {
        JsonObject jsonAuthInfo = new JsonObject();

        authInfo.stream().forEach(entry -> {
            jsonAuthInfo.put(entry.getKey(), entry.getValue());
        });

        authProvider.authenticate(jsonAuthInfo, vertxRes -> {
            BsonObject result = new BsonObject();
            if (vertxRes.succeeded()) {
                result.put(Codec.RESPONSE_OK, true);
            } else {
                result.put(Codec.RESPONSE_OK, false);
                result.put(Codec.RESPONSE_ERRMSG, vertxRes.cause().getMessage());
            }
            consumer.accept(result);
        });
    }
}
