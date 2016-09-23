package com.tesco.mubase.server;

import com.tesco.mubase.common.ReceivedEvent;
import com.tesco.mubase.common.FunctionContext;
import com.tesco.mubase.common.SubDescriptor;

import java.util.function.BiConsumer;

/**
 * Created by tim on 21/09/16.
 */
public interface Server {

    // Functions

    void installFunction(String functionName, SubDescriptor descriptor, BiConsumer<FunctionContext, ReceivedEvent> function);

    void uninstallFunction(String functionName);

    // Indexes

    void start();

    void createBinder(String binderName);

}
