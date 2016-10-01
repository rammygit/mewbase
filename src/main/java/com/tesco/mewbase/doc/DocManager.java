package com.tesco.mewbase.doc;

import com.tesco.mewbase.common.DocQuerier;
import com.tesco.mewbase.common.DocUpdater;

/**
 * Created by tim on 30/09/16.
 */
public interface DocManager extends DocQuerier, DocUpdater {

    String ID_FIELD = "id";
}
