package com.tesco.mewbase.example;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.bson.BsonPath;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;

/**
 * Created by tim on 08/11/16.
 */
public class ShoppingBasketExample {

    public static void main(String[] args) {
        try {
            new ShoppingBasketExample().example();
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    Simple shopping basket example
     */
    private void example() throws Exception {

        // Setup and start a server
        ServerOptions options =
                new ServerOptions().setChannels(new String[]{"orders"}).setBinders(new String[]{"baskets"});
        Server server = Server.newServer(options);
        server.start().get();

        // Register a projection that will respond to add_item events and increase/decrease the quantity of the item in the basket
        server.buildProjection("maintain_basket")                             // projection name
                .projecting("orders")                                           // channel name
                .filteredBy(ev -> ev.getString("eventType").equals("add_item")) // event filter
                .onto("baskets")                                                // binder name
                .identifiedBy(ev -> ev.getString("basketID"))                   // document id selector; how to obtain the doc id from the event bson
                .as((basket, del) ->                                            // projection function
                        BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID"))).register();

        // Create a client
        Client client = Client.newClient(new ClientOptions());

        // Send some add/remove events

        BsonObject event = new BsonObject().put("eventType", "add_item").put("basketID", "basket1111");

        client.publish("orders", event.copy().put("productID", "prod1234").put("quantity", 2));
        client.publish("orders", event.copy().put("productID", "prod2341").put("quantity", 1));
        client.publish("orders", event.copy().put("productID", "prod5432").put("quantity", 3));
        client.publish("orders", event.copy().put("productID", "prod5432").put("quantity", -1));

        Thread.sleep(1000);

        // Now get the basket
        BsonObject basket = client.findByID("baskets", "basket1111").get();

        System.out.println("Basket is: " + basket);
    }
}
