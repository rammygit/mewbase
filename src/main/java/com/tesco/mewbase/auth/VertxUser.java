package com.tesco.mewbase.auth;

import io.vertx.ext.auth.User;

public class VertxUser implements MewbaseUser {

    private User user;

    public VertxUser(User user) {
        this.user = user;
    }
}
