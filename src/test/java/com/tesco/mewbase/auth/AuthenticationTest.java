package com.tesco.mewbase.auth;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class AuthenticationTest extends AuthenticationTestBase {

    @Test
    public void testSuccessfulAuthentication(TestContext context) throws Exception {
        BsonObject authInfo = new BsonObject().put("success", true);
        execSimplePubSub(true, context, authInfo);
    }

    @Test
    public void testFailedAuthentication(TestContext context) throws Exception {
        BsonObject authInfo = new BsonObject().put("success", false);
        execSimplePubSub(false, context, authInfo);
    }

}
