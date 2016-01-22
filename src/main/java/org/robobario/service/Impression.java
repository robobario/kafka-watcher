package org.robobario.service;

import org.apache.avro.generic.GenericRecord;

import java.util.Collection;

public class Impression {

    private final String key;

    private final Collection<GenericRecord> value;


    public Impression(String key, Collection<GenericRecord> value) {
        this.key = key;
        this.value = value;
    }


    public String getKey() {
        return key;
    }


    public Collection<GenericRecord> getValue() {
        return value;
    }
}
