package com.tesco.mewbase.function;

import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.common.SubDescriptor;

import java.util.function.BiConsumer;

/**
 * Created by tim on 30/09/16.
 */
public interface FunctionManager {

    boolean installFunction(String functionName, SubDescriptor descriptor, BiConsumer<FunctionContext, Delivery> function);

    boolean deleteFunction(String functionName);

}
