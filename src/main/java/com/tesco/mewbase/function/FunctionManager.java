package com.tesco.mewbase.function;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.common.SubDescriptor;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 30/09/16.
 */
public interface FunctionManager {

    boolean installFunction(String name, SubDescriptor descriptor,
                            String binderName, String docIDField,
                            BiFunction<BsonObject, Delivery, BsonObject> function);

    boolean deleteFunction(String functionName);

}
