package com.tesco.mewbase.projection;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 28/11/16.
 */
public interface ProjectionBuilder {

    ProjectionBuilder projecting(String channelName);

    ProjectionBuilder filteredBy(Function<BsonObject, Boolean> eventFilter);

    ProjectionBuilder onto(String binderName);

    ProjectionBuilder identifiedBy(Function<BsonObject, String> docIDSelector);

    ProjectionBuilder as(BiFunction<BsonObject, Delivery, BsonObject> projectionFunction);

    Projection register();
}
