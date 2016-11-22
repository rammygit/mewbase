package com.tesco.mewbase.example.pubsub;

import com.tesco.mewbase.auth.DummyAuthProvider;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;

/**
 * Created by tim on 08/11/16.
 */
public class PubSubExample {

    private static final String USERNAME = "pub";
    private static final String PASSWORD = "sub";

    public static void main(String[] args) {
        try {
            new PubSubExample().example();
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    Very simple example showing pub/sub messaging
     */
    private void example() throws Exception {

        // Setup and start a server
        ServerOptions options = new ServerOptions().setChannels(new String[]{"orders"}).setAuthProvider(new DummyAuthProvider(USERNAME, PASSWORD));
        Server server = Server.newServer(options);
        server.start().get();

        BsonObject authInfo = new BsonObject();
        authInfo.put("username", USERNAME);
        authInfo.put("password", PASSWORD);

        // Create a client
        Client client = Client.newClient(new ClientOptions().setAuthInfo(authInfo));

        // Subscribe to a channel
        SubDescriptor descriptor = new SubDescriptor().setChannel("orders");
        client.subscribe(descriptor, del -> {
            System.
                    out.println("Received event: " + del.event().getString("foo"));
        });

        // Publish to the channel
        client.publish("orders", new BsonObject().put("foo", "bar"));
    }
}
