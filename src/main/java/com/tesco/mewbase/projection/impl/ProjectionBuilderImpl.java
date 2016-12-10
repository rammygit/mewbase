package com.tesco.mewbase.projection.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.projection.Projection;
import com.tesco.mewbase.projection.ProjectionBuilder;
import com.tesco.mewbase.projection.ProjectionManager;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 28/11/16.
 */
public class ProjectionBuilderImpl implements ProjectionBuilder {

    private final String projectionName;
    private final ProjectionManager projectionManager;
    private String channelName;
    private Function<BsonObject, Boolean> eventFilter = doc -> true;
    private String binderName;
    private Function<BsonObject, String> docIDSelector;
    private BiFunction<BsonObject, Delivery, BsonObject> projectionFunction;

    public ProjectionBuilderImpl(String projectionName, ProjectionManager projectionManager) {
        this.projectionName = projectionName;
        this.projectionManager = projectionManager;
    }

    @Override
    public ProjectionBuilder projecting(String channelName) {
        this.channelName = channelName;
        return this;
    }

    @Override
    public ProjectionBuilder filteredBy(Function<BsonObject, Boolean> eventFilter) {
        this.eventFilter = eventFilter;
        return this;
    }

    @Override
    public ProjectionBuilder onto(String binderName) {
        this.binderName = binderName;
        return this;
    }

    @Override
    public ProjectionBuilder identifiedBy(Function<BsonObject, String> docIDSelector) {
        this.docIDSelector = docIDSelector;
        return this;
    }

    @Override
    public ProjectionBuilder as(BiFunction<BsonObject, Delivery, BsonObject> projectionFunction) {
        this.projectionFunction = projectionFunction;
        return this;
    }

    @Override
    public Projection register() {
        if (channelName == null) {
            throw new IllegalStateException("Please specify a channel name");
        }
        if (eventFilter == null) {
            throw new IllegalStateException("Please specify an event filter");
        }
        if (binderName == null) {
            throw new IllegalStateException("Please specify a binder name");
        }
        if (docIDSelector == null) {
            throw new IllegalStateException("Please specify a document ID filter");
        }
        if (projectionFunction == null) {
            throw new IllegalStateException("Please specify a projection function");
        }

        return projectionManager.registerProjection(projectionName, channelName, eventFilter, binderName,
                docIDSelector, projectionFunction);
    }
}
