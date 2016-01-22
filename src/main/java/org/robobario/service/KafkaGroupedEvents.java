package org.robobario.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class KafkaGroupedEvents {

    private final Object thing = 1L;

    private Map<String, Collection<GenericRecord>> groups = Maps.newLinkedHashMap();


    public void add(GenericRecord record) {
        synchronized (thing) {
            String key = record.get("impressionId").toString();
            Collection<GenericRecord> records = get(key);
            records.add(record);
            if (groups.size() > 100) {
                groups.remove(groups.keySet().iterator().next());
            }
        }
    }


    public Impression getLatest() {
        synchronized (thing) {
            Set<Map.Entry<String, Collection<GenericRecord>>> entries = groups.entrySet();
            Map.Entry<String, Collection<GenericRecord>> entry = Iterables.getLast(entries, null);
            return entry == null ? null : new Impression(entry.getKey(), entry.getValue());
        }
    }


    public Impression getImpression(String impressionId) {
        synchronized (thing) {
            Set<Map.Entry<String, Collection<GenericRecord>>> entries = groups.entrySet();
            Optional<Map.Entry<String, Collection<GenericRecord>>> match = entries
                .stream()
                .filter(e -> Objects.equals(impressionId, e.getKey()))
                .findFirst();
            return match.isPresent() ? new Impression(match.get().getKey(), match.get().getValue()) : null;
        }
    }


    public Impression getNext(String impressionId) {
        synchronized (thing) {
            ImmutableList<String> ids = ImmutableList.copyOf(groups.keySet());
            if (ids.contains(impressionId) && ids.indexOf(impressionId) != ids.size() - 1) {
                String nextKey = ids.get(ids.indexOf(impressionId) + 1);
                Collection<GenericRecord> genericRecords = groups.get(nextKey);
                return new Impression(nextKey, genericRecords);
            }
            else {
                Set<Map.Entry<String, Collection<GenericRecord>>> entries = groups.entrySet();
                Map.Entry<String, Collection<GenericRecord>> entry = Iterables.getLast(entries, null);
                return entry == null ? null : new Impression(entry.getKey(), entry.getValue());
            }
        }
    }


    public Impression getPrevious(String impressionId) {
        synchronized (thing) {
            ImmutableList<String> ids = ImmutableList.copyOf(groups.keySet());
            if (ids.contains(impressionId) && ids.indexOf(impressionId) != 0) {
                String nextKey = ids.get(ids.indexOf(impressionId) - 1);
                Collection<GenericRecord> genericRecords = groups.get(nextKey);
                return new Impression(nextKey, genericRecords);
            }
            else {
                Set<Map.Entry<String, Collection<GenericRecord>>> entries = groups.entrySet();
                Map.Entry<String, Collection<GenericRecord>> entry = Iterables.getFirst(entries, null);
                return entry == null ? null : new Impression(entry.getKey(), entry.getValue());
            }
        }
    }


    private Collection<GenericRecord> get(String key) {
        Collection<GenericRecord> genericRecords = groups.get(key);
        if (genericRecords == null) {
            genericRecords = new ArrayList<GenericRecord>();
            groups.put(key, genericRecords);
        }
        return genericRecords;
    }


    public boolean hasNext(String impressionId) {
        synchronized (thing) {
            ImmutableList<String> ids = ImmutableList.copyOf(groups.keySet());
            if (!ids.contains(impressionId) || ids.indexOf(impressionId) != ids.size() - 1) {
                return true;
            }
        }
        return false;
    }
}
