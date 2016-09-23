package com.tesco.mubase.server.impl;

import com.tesco.mubase.common.ReceivedEvent;
import com.tesco.mubase.common.FunctionContext;
import com.tesco.mubase.common.SubDescriptor;
import com.tesco.mubase.server.Server;

import java.util.function.BiConsumer;

/**
 * Created by tim on 22/09/16.
 */
public class ServerImpl implements Server {
    @Override
    public void installFunction(String functionName, SubDescriptor descriptor, BiConsumer<FunctionContext, ReceivedEvent> function) {

    }

    @Override
    public void uninstallFunction(String functionName) {

    }

    @Override
    public void start() {

    }

    @Override
    public void createBinder(String binderName) {

    }
}
