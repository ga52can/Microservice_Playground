package com.sebis.mobility.model;

import org.springframework.ui.Model;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sohaib on 30/03/17.
 */
public class SimpleModel implements Model {

    private HashMap<String, Object> map = new HashMap<>();

    @Override
    public Model addAttribute(String s, Object o) {
        if (s != null && o != null) {
            map.put(s, o);
        }
        return this;
    }

    @Override
    public Model addAttribute(Object o) {
        return null;
    }

    @Override
    public Model addAllAttributes(Collection<?> collection) {
        return null;
    }

    @Override
    public Model addAllAttributes(Map<String, ?> map) {
        this.map.putAll(map);
        return null;
    }

    @Override
    public Model mergeAttributes(Map<String, ?> map) {
        return addAttribute(map);
    }

    @Override
    public boolean containsAttribute(String s) {
        return map.containsKey(s);
    }

    @Override
    public Map<String, Object> asMap() {
        return map;
    }
}
