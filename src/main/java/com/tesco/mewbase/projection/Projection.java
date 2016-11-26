package com.tesco.mewbase.projection;

/**
 * Created by tim on 25/11/16.
 */
public interface Projection {

    String getName();

    void pause();

    void resume();

    void unregister();

    void delete();
}
