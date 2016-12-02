package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 *
 * User-facing interface for authenticating users.
 *
 */
public interface MewbaseAuthProvider {

    /**
     * Authenticate a user.
     * <p>
     * The first argument is a Bson object containing information for authenticating the user. What this actually contains
     * depends on the specific implementation. In the case of a simple username/password based
     * authentication it is likely to contain a JSON object with the following structure:
     * <pre>
     *   {
     *     "username": "tim",
     *     "password": "mypassword"
     *   }
     * </pre>
     *
     * @param authInfo the auth information
     * @return a CompletableFuture containing a MewbaseUser where authorization operations can be performed
     *         on it
     */
    CompletableFuture<MewbaseUser> authenticate(BsonObject authInfo);
}
