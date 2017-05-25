package com.bikeemotion.quartz.jobstore.hazelcast.predicates;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.quartz.JobKey;

import com.bikeemotion.quartz.jobstore.hazelcast.utils.JsonEnvelope;
import com.bikeemotion.quartz.jobstore.hazelcast.utils.JsonHelper;
import com.hazelcast.query.Predicate;

/**
 * Filter triggers with a given job key
 */
public class TriggerByJobPredicate implements Predicate<JobKey, Object> {

    /**
     * 
     */
    private static final long serialVersionUID = 4152897021874488063L;
    private JobKey key;

    public TriggerByJobPredicate(JobKey key) {

        this.key = key;
    }

    @Override
    public boolean apply (Entry<JobKey, Object> entry) {

        
        Object v = entry.getValue();
        if (!(v instanceof JsonEnvelope)) {
            return false;
        }

        try {
            JsonEnvelope envelope = (JsonEnvelope) v;
            Map<String, Object> properties = JsonHelper.toPropertyMap(envelope.getJsonValue());
            
            return key.equals(new JobKey((String)properties.get("jobKeyName"), (String)properties.get("jobKeyGroup")));


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    
    }

}
