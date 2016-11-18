package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.server.impl.Codec;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

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
