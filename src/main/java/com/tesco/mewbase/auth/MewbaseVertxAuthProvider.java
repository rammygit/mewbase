package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

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
    public CompletableFuture<MewbaseUser> authenticate(BsonObject authInfo) {
        CompletableFuture<MewbaseUser> cf = new CompletableFuture<>();

        JsonObject jsonAuthInfo = new JsonObject();
        authInfo.stream().forEach(entry -> {
            jsonAuthInfo.put(entry.getKey(), entry.getValue());
        });

        authProvider.authenticate(jsonAuthInfo, vertxRes -> {
            if (vertxRes.succeeded()) {
                cf.complete(new VertxUser(vertxRes.result()));
            } else {
                cf.completeExceptionally(vertxRes.cause());
            }
        });

        return cf;
    }
}
