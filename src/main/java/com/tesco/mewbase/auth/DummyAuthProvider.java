package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.server.impl.Codec;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class DummyAuthProvider implements MewbaseAuthProvider {

    @Override
    public CompletableFuture<BsonObject> authenticate(BsonObject authInfo) {
        CompletableFuture cf = new CompletableFuture();

        BsonObject result = new BsonObject();
        result.put(Codec.RESPONSE_OK, true);

        cf.complete(result);

        return cf;
    }
}
