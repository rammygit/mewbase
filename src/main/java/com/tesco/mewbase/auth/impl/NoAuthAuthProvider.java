package com.tesco.mewbase.auth.impl;

import com.tesco.mewbase.auth.MewbaseAuthProvider;
import com.tesco.mewbase.auth.MewbaseUser;
import com.tesco.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * The default auth provider implementation that does not enforce any authentication
 */
public class NoAuthAuthProvider implements MewbaseAuthProvider {

    @Override
    public CompletableFuture<MewbaseUser> authenticate(BsonObject authInfo) {
        return CompletableFuture.completedFuture(new DummyUser());
    }
}
