package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;

/**
 * Created by tim on 22/09/16.
 */
public class SubDescriptor {

    private String durableID;
    private String streamName;
    private Long startSeq;
    private long startTimestamp;
    private BsonObject matcher;

    public String getDurableID() {
        return durableID;
    }

    public SubDescriptor setDurableID(String durableID) {
        this.durableID = durableID;
        return this;
    }

    public String getStreamName() {
        return streamName;
    }

    public SubDescriptor setStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public Long getStartSeq() {
        return startSeq;
    }

    public SubDescriptor setStartSeq(long startSeq) {
        this.startSeq = startSeq;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubDescriptor that = (SubDescriptor) o;

        if (startTimestamp != that.startTimestamp) return false;
        if (durableID != null ? !durableID.equals(that.durableID) : that.durableID != null) return false;
        if (streamName != null ? !streamName.equals(that.streamName) : that.streamName != null) return false;
        if (startSeq != null ? !startSeq.equals(that.startSeq) : that.startSeq != null)
            return false;
        return matcher != null ? matcher.equals(that.matcher) : that.matcher == null;

    }

    @Override
    public int hashCode() {
        int result = durableID != null ? durableID.hashCode() : 0;
        result = 31 * result + (streamName != null ? streamName.hashCode() : 0);
        result = 31 * result + (startSeq != null ? startSeq.hashCode() : 0);
        result = 31 * result + (int) (startTimestamp ^ (startTimestamp >>> 32));
        result = 31 * result + (matcher != null ? matcher.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SubDescriptor{" +
                "durableID='" + durableID + '\'' +
                ", streamName='" + streamName + '\'' +
                ", startSeq=" + startSeq +
                ", startTimestamp=" + startTimestamp +
                ", matcher=" + matcher +
                '}';
    }
}
