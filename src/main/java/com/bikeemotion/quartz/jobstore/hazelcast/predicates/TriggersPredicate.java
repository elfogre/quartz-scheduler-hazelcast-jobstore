package com.bikeemotion.quartz.jobstore.hazelcast.predicates;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.quartz.TriggerKey;

import com.bikeemotion.quartz.jobstore.hazelcast.utils.JsonEnvelope;
import com.bikeemotion.quartz.jobstore.hazelcast.utils.JsonHelper;
import com.hazelcast.query.Predicate;

/**
 * Filter triggers with a given fire start time, end time and state.
 */
public class TriggersPredicate implements Predicate<TriggerKey, Object> {

    /**
     * 
     */
    private static final long serialVersionUID = 5318036582558676659L;
    long noLaterThanWithTimeWindow;

    public TriggersPredicate(long noLaterThanWithTimeWindow) {

        this.noLaterThanWithTimeWindow = noLaterThanWithTimeWindow;
    }

    @Override
    public boolean apply (Entry<TriggerKey, Object> entry) {

        Object v = entry.getValue();
        if (v == null || ( !(v instanceof JsonEnvelope))) {
            return false;
        }

        try {
            JsonEnvelope envelope = (JsonEnvelope) v;
            Map<String, Object> properties = JsonHelper.toPropertyMap(envelope.getJsonValue());
            long nextFireTime = (long) properties.get("nextFireTime");

            return nextFireTime <= noLaterThanWithTimeWindow;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
