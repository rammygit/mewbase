package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.server.impl.Codec;

import java.util.concurrent.CompletableFuture;

public class TestAuthProvider implements MewbaseAuthProvider {

    @Override
    public CompletableFuture<BsonObject> authenticate(BsonObject authInfo) {
        CompletableFuture cf = new CompletableFuture();

        boolean success = authInfo.getBoolean("success");

        BsonObject resp = new BsonObject();
        resp.put(Codec.RESPONSE_OK, success);

        if (!success) {
            resp.put(Codec.RESPONSE_ERRCODE, "ERR_AUTH_001");
            resp.put(Codec.RESPONSE_ERRMSG, "Not authenticated");
        }

        cf.complete(resp);
        return cf;
    }
}
