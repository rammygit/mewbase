package com.tesco.mubase;

import com.tesco.mubase.client.Client;
import com.tesco.mubase.common.BsonObject;
import com.tesco.mubase.common.EventHolder;
import com.tesco.mubase.common.FunctionContext;
import com.tesco.mubase.common.SubDescriptor;
import com.tesco.mubase.server.Server;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Created by tim on 22/09/16.
 */
public class MubaseTest {

    @Test
    public void testFunction() {
        Server server = null;
        Client mubase = null;

        BiConsumer<FunctionContext, EventHolder> function = (ctx, ev) -> {
            String customerID = ev.event().getString("CustomerID");
            String productID = ev.event().getString("ProductID");
            int quantity = ev.event().getInt("Quantity");

            CompletableFuture<BsonObject> fut = ctx.getDocument("baskets", customerID);
            fut.thenAccept(doc -> {
                Integer currQ = doc.getInt(productID);
                if (currQ == null) {
                    currQ = 0;
                }
                currQ += quantity;
                // TODO set the value

                // And save it
            });
        };

        SubDescriptor descriptor = null;

        server.installFunction("myfunc", descriptor, function);
    }
}
