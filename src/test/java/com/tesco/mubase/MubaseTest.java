package com.tesco.mubase;

import org.junit.Test;

/**
 * Created by tim on 22/09/16.
 */
public class MubaseTest {

    @Test
    public void testFunction() {
//        Server server = null;
//        Client mubase = null;
//
//        BiConsumer<FunctionContext, ReceivedEvent> function = (ctx, ev) -> {
//            String customerID = ev.event().getString("CustomerID");
//            String productID = ev.event().getString("ProductID");
//            int quantity = ev.event().getInt("Quantity");
//
//            CompletableFuture<BsonObject> fut = ctx.getDocument("baskets", customerID);
//            fut.thenAccept(doc -> {
//                Integer currQ = doc.getInt(productID);
//                if (currQ == null) {
//                    currQ = 0;
//                }
//                currQ += quantity;
//                // TODO set the value
//
//                // And save it
//            });
//        };
//
//        SubDescriptor descriptor = null;
//
//        server.installFunction("myfunc", descriptor, function);
    }
}
