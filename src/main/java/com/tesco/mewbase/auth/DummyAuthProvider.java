package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.server.impl.Codec;

import java.util.function.Consumer;

public class DummyAuthProvider implements MewbaseAuthProvider {

    private String username;
    private String password;

    public DummyAuthProvider(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public void authenticate(BsonObject authInfo, Consumer<BsonObject> consumer) {
        String username = authInfo.getString("username");
        String password = authInfo.getString("password");

        BsonObject result = new BsonObject();
        if (!username.equals(this.username) || !password.equals(this.password)) {
            result.put(Codec.RESPONSE_OK, false);
            result.put(Codec.RESPONSE_ERRMSG, "Incorrect username/password");
        } else {
            result.put(Codec.RESPONSE_OK, true);
        }
        consumer.accept(result);
    }
}
