package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;

/**
 * Created by tim on 22/09/16.
 */
public class SubDescriptor {

    public static final long DEFAULT_START_POS = -1;

    private String channel;
    private String durableID;
    private long startPos = DEFAULT_START_POS;
    private long startTimestamp;
    private BsonObject matcher;
    private String group;

    public String getDurableID() {
        return durableID;
    }

    public SubDescriptor setDurableID(String durableID) {
        this.durableID = durableID;
        return this;
    }

    public String getChannel() {
        return channel;
    }

    public SubDescriptor setChannel(String channel) {
        this.channel = channel;
        return this;
    }

    public long getStartPos() {
        return startPos;
    }

    public SubDescriptor setStartPos(long startPos) {
        this.startPos = startPos;
        return this;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public SubDescriptor setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
        return this;
    }

    public BsonObject getMatcher() {
        return matcher;
    }

    public SubDescriptor setMatcher(BsonObject matcher) {
        this.matcher = matcher;
        return this;
    }


    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubDescriptor that = (SubDescriptor)o;

        if (startTimestamp != that.startTimestamp) return false;
        if (startPos != that.startPos) return false;
        if (durableID != null ? !durableID.equals(that.durableID) : that.durableID != null) return false;
        if (channel != null ? !channel.equals(that.channel) : that.channel != null) return false;
        if (matcher != null ? !matcher.equals(that.matcher) : that.matcher != null) return false;
        return group != null ? group.equals(that.group) : that.group == null;

    }

    @Override
    public int hashCode() {
        int result = durableID != null ? durableID.hashCode() : 0;
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (int)(startPos ^ (startPos >>> 32));
        result = 31 * result + (int)(startTimestamp ^ (startTimestamp >>> 32));
        result = 31 * result + (matcher != null ? matcher.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        return result;
    }
}
