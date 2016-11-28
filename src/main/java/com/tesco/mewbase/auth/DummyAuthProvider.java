package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

public class DummyAuthProvider implements MewbaseAuthProvider {

    @Override
    public CompletableFuture<MewbaseUser> authenticate(BsonObject authInfo) {
        CompletableFuture cf = new CompletableFuture();

        return cf.completedFuture(new DummyUser());
    }
}
