package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

@RunWith(VertxUnitRunner.class)
public class AuthenticationTest extends AuthenticationTestBase {

    @Test
    public void testSuccessfulAuthentication(TestContext context) throws Exception {
        BsonObject authInfo = new BsonObject().put("success", true);
        execSimplePubSub(context, authInfo);
    }

    @Test
    public void testFailedAuthentication(TestContext context) throws Exception {
        thrown.expect(ExecutionException.class);
        //TODO: When error messages and codes are centralized this should be changed
        thrown.expectMessage("Incorrect username/password");

        BsonObject authInfo = new BsonObject().put("success", false);
        execSimplePubSub(context, authInfo);
    }
}
