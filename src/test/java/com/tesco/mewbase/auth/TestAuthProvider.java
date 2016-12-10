package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;

import java.util.concurrent.CompletableFuture;

public class TestAuthProvider implements MewbaseAuthProvider {

    @Override
    public CompletableFuture<MewbaseUser> authenticate(BsonObject authInfo) {
        CompletableFuture cf = new CompletableFuture();

        boolean success = authInfo.getBoolean("success");

        if (!success) {
            cf.completeExceptionally(new MewException("Incorrect username/password"));
        } else {
            cf.complete(new TestUser());
        }

        return cf;
    }
}
