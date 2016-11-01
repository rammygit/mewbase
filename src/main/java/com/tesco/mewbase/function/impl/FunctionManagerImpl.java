package com.tesco.mewbase.function.impl;

import com.tesco.mewbase.client.*;
import com.tesco.mewbase.client.impl.ClientImpl;
import com.tesco.mewbase.common.ReceivedEvent;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.function.FunctionContext;
import com.tesco.mewbase.function.FunctionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Created by tim on 30/09/16.
 */
public class FunctionManagerImpl implements FunctionManager {

    private final static Logger log = LoggerFactory.getLogger(FunctionManagerImpl.class);

    private final Map<String, FuncHolder> functions = new ConcurrentHashMap<>();
    private final DocManager docManager;
    private final Client localClient; // TODO this should use invm connection

    public FunctionManagerImpl(DocManager docManager) {
        this.docManager = docManager;
        this.localClient = Client.newClient();
    }

    @Override
    public boolean installFunction(String functionName, SubDescriptor descriptor,
                                   BiConsumer<FunctionContext, ReceivedEvent> function) {
        try {
            // TODO blocking!
            Connection connection = localClient.connect(new ConnectionOptions()).get();
            FuncHolder holder = new FuncHolder(new FunctionContextImpl(docManager), function);
            if (functions.putIfAbsent(functionName, holder) != null) {
                return false;
            }

            holder.init(connection, descriptor);
        } catch (Exception e) {
            throw new MewException(e.getMessage(), e);
        }
        return true;
    }

    @Override
    public boolean deleteFunction(String functionName) {
        FuncHolder holder = functions.remove(functionName);
        if (holder != null) {
            holder.close();
            return true;
        } else {
            return false;
        }
    }

    private static final class FuncHolder {
        final FunctionContext ctx;
        final BiConsumer<FunctionContext, ReceivedEvent> function;
        volatile Subscription sub;

        public FuncHolder(FunctionContext ctx, BiConsumer<FunctionContext, ReceivedEvent> function) {
            this.ctx = ctx;
            this.function = function;
        }

        void init(Connection connection, SubDescriptor subDescriptor) {
            connection.subscribe(subDescriptor).handle((sub, t) -> {
                if (t != null) {
                    //TODO handle properly
                    log.error("Failed to subscribe function", t);
                } else {
                    FuncHolder.this.sub = sub;
                    sub.setHandler(re -> {
                        try {
                            function.accept(ctx, re);
                        } catch (Throwable t2) {
                            // TODO do something
                        }
                    });
                }
                return null;
            });
        }

        void close() {
            sub.unsubscribe();
        }
    }

}
