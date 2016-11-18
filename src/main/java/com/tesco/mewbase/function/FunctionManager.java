package com.tesco.mewbase.function;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 30/09/16.
 */
public interface FunctionManager {

    boolean installFunction(String name, String channel, Function<BsonObject, Boolean> eventFilter,
                            String binderName, Function<BsonObject, String> docIDSelector,
                            BiFunction<BsonObject, Delivery, BsonObject> function);

    boolean deleteFunction(String functionName);

}
