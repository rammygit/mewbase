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
    public synchronized boolean installFunction(String name, SubDescriptor descriptor, String binderName,
                                                String docIDField, BiFunction<BsonObject, Delivery, BsonObject> function) {
        if (functions.containsKey(name)) {
            return false;
        }
        FuncHolder holder = new FuncHolder(descriptor, binderName, docIDField, function);
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

        final SubDescriptor descriptor;
        final String binderName;
        final String docIDField;
        final ReadStream readStream;
        final  BiFunction<BsonObject, Delivery, BsonObject> function;

        public FuncHolder(SubDescriptor subDescriptor, String binderName, String docIDField,
                          BiFunction<BsonObject, Delivery, BsonObject> function) {
            this.descriptor = subDescriptor;
            this.binderName = binderName;
            this.docIDField = docIDField;
            this.function = function;
            Log log = logManager.getLog(descriptor.getChannel());
            this.readStream = log.subscribe(descriptor);
            readStream.handler(this::handler);
            readStream.start();
        }

        void handler(long seq, BsonObject frame) {
            frame.put(Codec.RECEV_POS, seq);
            BsonObject event = frame.getBsonObject(Codec.RECEV_EVENT);
            String docID = event.getString(docIDField);
            if (docID == null) {
                throw new IllegalArgumentException("No field " + docIDField + " in event " + event);
            }
            docManager.findByID(binderName, docID).handle((doc, t) -> {
                if (t == null) {
                    try {
                        if (doc == null) {
                            doc = new BsonObject().put(ID_FIELD, docID);
                        }
                        Delivery delivery = new DeliveryImpl(descriptor.getChannel(), frame.getLong(Codec.RECEV_TIMESTAMP),
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
