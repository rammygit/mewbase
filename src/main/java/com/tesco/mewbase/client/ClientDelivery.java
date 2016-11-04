package com.tesco.mewbase.client;

import com.tesco.mewbase.common.Delivery;

/**
 * Created by tim on 04/11/16.
 */
public interface ClientDelivery extends Delivery {

    void acknowledge();

    Subscription subscription();
}
