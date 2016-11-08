package com.tesco.mewbase.function.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.common.impl.DeliveryImpl;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.function.FunctionManager;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogManager;
import com.tesco.mewbase.server.impl.Codec;
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
public class FunctionManagerImpl implements FunctionManager {

    private final static Logger log = LoggerFactory.getLogger(FunctionManagerImpl.class);

    private final Map<String, FuncHolder> functions = new HashMap<>();

    private final DocManager docManager;
    private final LogManager logManager;

    public FunctionManagerImpl(DocManager docManager, LogManager logManager) {
        this.docManager = docManager;
        this.logManager = logManager;
    }

    @Override
    public boolean installFunction(String name, String channel, Function<BsonObject, Boolean> eventFilter,
                                   String binderName, Function<BsonObject, String> docIDSelector,
                                   BiFunction<BsonObject, Delivery, BsonObject> function) {
        if (functions.containsKey(name)) {
            return false;
        }
        FuncHolder holder = new FuncHolder(channel, binderName, eventFilter, docIDSelector, function);
        functions.put(name, holder);
        return true;
    }

    @Override
    public synchronized boolean deleteFunction(String functionName) {
        FuncHolder holder = functions.remove(functionName);
        if (holder != null) {
            holder.close();
            return true;
        } else {
            return false;
        }
    }

    private class FuncHolder {

        final String channel;
        final String binderName;
        final ReadStream readStream;
        final Function<BsonObject, Boolean> eventFilter;
        final Function<BsonObject, String> docIDSelector;
        final BiFunction<BsonObject, Delivery, BsonObject> function;

        public FuncHolder(String channel, String binderName, Function<BsonObject, Boolean> eventFilter,
                          Function<BsonObject, String> docIDSelector,
                          BiFunction<BsonObject, Delivery, BsonObject> function) {
            this.channel = channel;
            this.binderName = binderName;
            this.eventFilter = eventFilter;
            this.docIDSelector = docIDSelector;
            this.function = function;
            Log log = logManager.getLog(channel);
            this.readStream = log.subscribe(new SubDescriptor().setChannel(channel));
            readStream.handler(this::handler);
            readStream.start();
        }

        void handler(long seq, BsonObject frame) {
            frame.put(Codec.RECEV_POS, seq);
            BsonObject event = frame.getBsonObject(Codec.RECEV_EVENT);
            if (!eventFilter.apply(event)) {
                return;
            }
            String docID = docIDSelector.apply(event);
            if (docID == null) {
                throw new IllegalArgumentException("No doc ID found in event " + event);
            }
            docManager.findByID(binderName, docID).handle((doc, t) -> {
                if (t == null) {
                    try {
                        if (doc == null) {
                            doc = new BsonObject().put(ID_FIELD, docID);
                        }
                        Delivery delivery = new DeliveryImpl(channel, frame.getLong(Codec.RECEV_TIMESTAMP),
                                frame.getLong(Codec.RECEV_POS), frame.getBsonObject(Codec.RECEV_EVENT));
                        BsonObject updated = function.apply(doc, delivery);
                        CompletableFuture<Void> cfSaved = docManager.save(binderName, docID, updated);
                        cfSaved.handle((v, t3) -> {
                            if (t3 == null) {
                                // Saved OK update last update sequence TODO
                            } else {
                                // TODO how to handle exceptions?
                                log.error("Failed to save document", t3);
                            }
                           return null;
                        });
                    } catch (Throwable t2) {
                        // TODO how to handle exceptions?
                        log.error("Failed to call function", t2);
                    }
                } else {
                    // TODO how to handle exceptions?
                    log.error("Failed to find document");
                }
                return null;
            });
        }

        void close() {
            readStream.close();
        }

    }

}
