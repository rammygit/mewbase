package com.tesco.mewbase.doc.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tim on 30/09/16.
 */
public class Binder {

    private final Map<String, BsonObject> docs = new ConcurrentHashMap<>();

    public BsonObject getDocument(String id) {
        return docs.get(id);
    }

    public void upsert(String id, BsonObject doc) {
        BsonObject curr = docs.get(id);
//
//
//        BsonObject inc = doc.getBsonObject("$inc");
//        if (inc != null) {
//            doc.remove("$inc");
//            // TODO hardcoded for now, implement properly
//            BsonObject items = curr.getBsonObject("items");
//            if (items == null) {
//                items = new BsonObject();
//                curr.put("items", items);
//            }
//            final BsonObject theItems = items;
//            inc.forEach(entry -> {
//                String fieldName = entry.getKey();
//                String[] split = fieldName.split(".");
//                String productID = split[1];
//                int quant = (int)entry.getValue();
//                Integer currQuant = theItems.getInteger(productID);
//                if (currQuant == null) {
//                    currQuant = 0;
//                }
//                currQuant += quant;
//                theItems.put(productID, currQuant);
//            });
//        }
//
        docs.put(id, doc);
    }
}
