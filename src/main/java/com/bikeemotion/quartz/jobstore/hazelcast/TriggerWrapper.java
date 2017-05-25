package com.bikeemotion.quartz.jobstore.hazelcast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.quartz.DateBuilder;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TriggerWrapper {

    final static int ID = 100;

    @JsonIgnore
    private TriggerKey key;

    private String keyName;
    private String keyGroup;

    @JsonIgnore
    private JobKey jobKey;

    private String jobKeyName;
    private String jobKeyGroup;

    @JsonIgnore
    private OperableTrigger trigger;
    
    @SuppressWarnings("unused")
    private byte[] triggerSerialized;

    private Long acquiredAt;

    private Long nextFireTime;

    private TriggerState state;

    private TriggerWrapper(OperableTrigger trigger, TriggerState state) {

        if (trigger == null) {
            throw new IllegalArgumentException("Trigger cannot be null!");
        }
        this.trigger = trigger;
        setKey(trigger.getKey());
        setKeyGroup(key.getGroup());
        setKeyName(key.getName());

        setJobKey(trigger.getJobKey());
        setJobKeyGroup(jobKey.getGroup());
        setJobKeyName(jobKey.getName());

        this.state = state;

        // Change to normal if acquired is not released in 5 seconds
        if (state == TriggerState.ACQUIRED) {
            acquiredAt = DateBuilder.newDate().build().getTime();
        } else {
            acquiredAt = null;
        }
    }

    public TriggerWrapper() {
        // TODO Auto-generated constructor stub
    }

    public static TriggerWrapper newTriggerWrapper (OperableTrigger trigger) {

        return newTriggerWrapper(trigger, TriggerState.NORMAL);
    }

    public static TriggerWrapper newTriggerWrapper (TriggerWrapper tw, TriggerState state) {

        return new TriggerWrapper(tw.trigger, state);
    }

    public static TriggerWrapper newTriggerWrapper (OperableTrigger trigger, TriggerState state) {

        TriggerWrapper tw = new TriggerWrapper(trigger, state);
        return tw;
    }

    public Long getNextFireTime () {
        if (trigger == null || trigger.getNextFireTime() == null) {
            if (nextFireTime == null) {
                return 0L;
            } else {
                return nextFireTime;
            }
        } else {
            return trigger.getNextFireTime().getTime();
        }
    }

    public void setNextFireTime (Long nextFireTime) {
        this.nextFireTime = nextFireTime;
    }

    public OperableTrigger getTrigger () {

        return this.trigger;
    }

    public TriggerState getState () {

        return state;
    }

    public Long getAcquiredAt () {
        if (acquiredAt == null) {
            return 0L;
        }
        return acquiredAt;
    }

    @Override
    public String toString () {

        return "TriggerWrapper{" + "trigger=" + trigger + ", state=" + state + ", nextFireTime=" + getNextFireTime() + ", acquiredAt="
            + getAcquiredAt() + '}';
    }

    @Override
    public boolean equals (Object obj) {

        if (obj instanceof TriggerWrapper) {
            TriggerWrapper tw = (TriggerWrapper) obj;
            if (tw.getKey().equals(this.getKey())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode () {

        return getKey().hashCode();
    }

    @JsonIgnore
    public TriggerKey getKey () {
        if (key == null) {
            key = new TriggerKey(keyName, keyGroup);
        }
        return key;
    }

    @JsonIgnore
    public void setKey (TriggerKey key) {
        this.key = key;
    }

    @JsonIgnore
    public JobKey getJobKey () {
        if (jobKey == null) {
            jobKey = new JobKey(jobKeyName, jobKeyGroup);
        }
        return jobKey;
    }

    @JsonIgnore
    public void setJobKey (JobKey jobKey) {
        this.jobKey = jobKey;
    }

    public void setTrigger (OperableTrigger trigger) {
        this.trigger =  trigger;
    }

    public void setAcquiredAt (Long acquiredAt) {
        this.acquiredAt = acquiredAt;
    }

    public void setState (TriggerState state) {
        this.state = state;
    }

    public String getKeyGroup () {
        return keyGroup;
    }

    public void setKeyGroup (String keyGroup) {
        this.keyGroup = keyGroup;
    }

    public String getKeyName () {
        return keyName;
    }

    public void setKeyName (String keyName) {
        this.keyName = keyName;
    }

    public String getJobKeyName () {
        return jobKeyName;
    }

    public void setJobKeyName (String jobKeyName) {
        this.jobKeyName = jobKeyName;
    }

    public String getJobKeyGroup () {
        return jobKeyGroup;
    }

    public void setJobKeyGroup (String jobKeyGroup) {
        this.jobKeyGroup = jobKeyGroup;
    }

    public byte[] getTriggerSerialized () throws IOException {
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(trigger);
        out.close();
        
        return bos.toByteArray();
    }

    public void setTriggerSerialized (byte[] triggerSerialized) throws IOException, ClassNotFoundException {
        this.triggerSerialized = triggerSerialized;
        
        ByteArrayInputStream bis = new ByteArrayInputStream(triggerSerialized);
        ObjectInputStream in = new ObjectInputStream(bis);
        this.trigger = (OperableTrigger) in.readObject();
    }

}
