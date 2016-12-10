package com.tesco.mewbase.projection;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 30/09/16.
 */
public interface ProjectionManager {

    /**
     * Installs a projection on the server that will process events on a channel matching a filter
     *
     * @param name               the name of the projection, is used to unregister projection at a later time, only one projection is allowed with any given name
     * @param channel            the name of the channel, a channel is published to and only event published to this channel will be available to this projection
     * @param eventFilter        filters out events that match the filter, that is return false
     * @param binderName         the binder where the documents are stored and updated to
     * @param docIDSelector      a projection that returns the document id form the bson
     * @param projectionFunction the projection to apply to the bson
     * @return true is the projection is installed successfully
     */
    Projection registerProjection(String name, String channel, Function<BsonObject, Boolean> eventFilter,
                                  String binderName, Function<BsonObject, String> docIDSelector,
                                  BiFunction<BsonObject, Delivery, BsonObject> projectionFunction);

    ProjectionBuilder buildProjection(String name);


}
