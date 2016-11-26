package com.tesco.mewbase.projection.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.common.impl.DeliveryImpl;
import com.tesco.mewbase.projection.Projection;
import com.tesco.mewbase.projection.ProjectionManager;
import com.tesco.mewbase.server.impl.Protocol;
import com.tesco.mewbase.server.impl.ServerImpl;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.tesco.mewbase.doc.DocManager.ID_FIELD;

/**
 * Created by tim on 30/09/16.
 */
public class ProjectionManagerImpl implements ProjectionManager {

    private final static Logger log = LoggerFactory.getLogger(ProjectionManagerImpl.class);

    public static final String PROJECTION_STATE_FIELD = "_mb.lastSeqs";

    private final Map<String, ProjectionImpl> projections = new HashMap<>();

    private final ServerImpl server;

    public ProjectionManagerImpl(ServerImpl server) {
        this.server = server;
    }

    @Override
    public Projection registerProjection(String name, String channel, Function<BsonObject, Boolean> eventFilter,
                                         String binderName, Function<BsonObject, String> docIDSelector,
                                         BiFunction<BsonObject, Delivery, BsonObject> projectionFunction) {
        if (projections.containsKey(name)) {
            throw new IllegalArgumentException("Projection " + name + " already registered");
        }
        log.trace("Registering projection " + name);
        ProjectionImpl holder =
                new ProjectionImpl(name, channel, binderName, eventFilter, docIDSelector, projectionFunction);
        projections.put(name, holder);
        return holder;
    }


    private class ProjectionImpl implements Projection {

        final String name;
        final String channel;
        final String binderName;
        final ProjectionSubscription subscription;
        final Function<BsonObject, Boolean> eventFilter;
        final Function<BsonObject, String> docIDSelector;
        final BiFunction<BsonObject, Delivery, BsonObject> projectionFunction;

        public ProjectionImpl(String name, String channel, String binderName, Function<BsonObject, Boolean> eventFilter,
                              Function<BsonObject, String> docIDSelector,
                              BiFunction<BsonObject, Delivery, BsonObject> projectionFunction) {
            this.name = name;
            this.channel = channel;
            this.binderName = binderName;
            this.eventFilter = eventFilter;
            this.docIDSelector = docIDSelector;
            this.projectionFunction = projectionFunction;
            SubDescriptor subDescriptor = new SubDescriptor().setChannel(channel).setDurableID(name);
            this.subscription = new ProjectionSubscription(server, subDescriptor, this::handler);
        }

        void handler(long seq, BsonObject frame) {
            BsonObject event = frame.getBsonObject(Protocol.RECEV_EVENT);

            // Apply event filter
            if (!eventFilter.apply(event)) {
                return;
            }

            String docID = docIDSelector.apply(event);
            if (docID == null) {
                throw new IllegalArgumentException("No doc ID found in event " + event);
            }

            // 1. get lock
            // Before loading the doc we need to get an async lock otherwise we might load it before a previous
            // update has completed
            // TODO this can be optimised
            AsyncResCF<Lock> cfLock = new AsyncResCF<>();
            String lockName = binderName + "." + docID;
            server.vertx().sharedData().getLock(lockName, cfLock);

            // 2. Get doc
            cfLock.thenCompose(l -> server.docManager().get(binderName, docID))

            // 3. duplicate detection and call projection function
            .thenCompose(doc -> {

                // Duplicate detection
                BsonObject lastSeqs = null;
                if (doc == null) {
                    doc = new BsonObject().put(ID_FIELD, docID);
                } else {
                    lastSeqs = doc.getBsonObject(PROJECTION_STATE_FIELD);
                    if (lastSeqs != null) {
                        Long processedSeq = lastSeqs.getLong(name);
                        if (processedSeq != null) {
                            if (processedSeq >= seq) {
                                // We've processed this one before, so ignore it
                                log.trace("Ignoring event " + seq + " as already processed");
                                return CompletableFuture.completedFuture(false);
                            }
                        }
                    }
                }

                if (lastSeqs == null) {
                    lastSeqs = new BsonObject();
                    doc.put(PROJECTION_STATE_FIELD, lastSeqs);
                }
                Delivery delivery = new DeliveryImpl(channel, frame.getLong(Protocol.RECEV_TIMESTAMP),
                        seq, frame.getBsonObject(Protocol.RECEV_EVENT));
                BsonObject updated = projectionFunction.apply(doc, delivery);

                // Update the last sequence
                lastSeqs.put(name, seq);

                // Store the doc
                CompletableFuture<Void> cfSaved = server.docManager().put(binderName, docID, updated);
                return cfSaved.thenApply(v -> true);
            })

            // 4. acknowledge and release lock if was processed
            .thenAccept(processed -> {
                if (processed) {
                    subscription.acknowledge(seq);
                }
                try {
                    cfLock.get().release();
                } catch (Exception e) {
                    throw new MewException(e);
                }
            })

            // 5. handle exceptions
            .exceptionally(t -> {
                log.error("Failed in processing projection " + name, t);
                return null;
            });
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void pause() {
            subscription.pause();
        }

        @Override
        public void resume() {
            subscription.resume();
        }

        @Override
        public void unregister() {
            projections.remove(name);
            subscription.close();
        }

        @Override
        public void delete() {
            unregister();
            subscription.unsubscribe();
        }
    }

}
