package com.tesco.mewbase.function;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 30/09/16.
 */
public interface FunctionManager {

    /**
     * Installs a function on the server that will process events on a channel matching a filter
     *
     * @param name the name of the function, is used to delete function at a later time, only one function is allowed with any given name
     * @param channel the name of the channel, a channel is published to and only event published to this channel will be available to this function
     * @param eventFilter filters out events that match the filter, that is return false
     * @param binderName the binder where the documents are stored and updated to
     * @param docIDSelector a function that returns the document id form the bson
     * @param function the function to apply to the bson
     * @return true is the function is installed successfully
     */
    boolean installFunction(String name, String channel, Function<BsonObject, Boolean> eventFilter,
                            String binderName, Function<BsonObject, String> docIDSelector,
                            BiFunction<BsonObject, Delivery, BsonObject> function);

    /**
     * Removes the function with the given name from the function manager
     *
     * @param functionName the name of the function to remove
     * @return true if the function is successfully removed
     */
    boolean deleteFunction(String functionName);

}
