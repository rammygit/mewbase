package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.server.impl.Codec;

import java.util.concurrent.CompletableFuture;

public class DummyAuthProvider implements MewbaseAuthProvider {

    @Override
    public CompletableFuture<MewbaseUser> authenticate(BsonObject authInfo) {
        CompletableFuture cf = new CompletableFuture();

        BsonObject result = new BsonObject();
        result.put(Codec.RESPONSE_OK, true);

        cf.complete(new DummyUser());

        return cf;
    }
}
