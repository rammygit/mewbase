package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;

import java.util.function.Consumer;

public interface MewbaseAuthProvider {

    void authenticate(BsonObject authInfo, Consumer<BsonObject> handler);
}
