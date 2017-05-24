package com.bikeemotion.quartz.jobstore.hazelcast;

import org.quartz.DateBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

public class TriggerWrapper implements Portable {

    final static int ID = 100;

    private TriggerKey key;

    private JobKey jobKey;

    private OperableTrigger trigger;

    private Long acquiredAt;

    private Long nextFireTime;

    private TriggerState state;

    private TriggerWrapper(OperableTrigger trigger, TriggerState state) {

        if (trigger == null) {
            throw new IllegalArgumentException("Trigger cannot be null!");
        }
        this.trigger = trigger;
        setKey(trigger.getKey());
        this.setJobKey(trigger.getJobKey());
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
            if (nextFireTime==null) {
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
    public int getClassId () {
        return ID;
    }

    @Override
    public int getFactoryId () {
        return 1;
    }

    @Override
    public void readPortable (PortableReader reader) throws IOException {
        this.setNextFireTime(reader.readLong("nextFireTime"));
        this.setAcquiredAt(reader.readLong("acquiredAt"));
        String keyString = reader.readUTF("key");
        if (keyString != null) {
            String[] keyParts = keyString.split("\\.", 2);
            if (keyParts.length==2) {
                this.setKey(new TriggerKey(keyParts[1], keyParts[0]));
            }
            if (keyParts.length==1) {
                this.setKey(new TriggerKey(keyParts[0]));
            }
        }
        keyString = reader.readUTF("jobKey");
        if (keyString != null) {
            String[] keyParts = keyString.split("\\.", 2);
            if (keyParts.length==2) {
                this.setJobKey(new JobKey(keyParts[1], keyParts[0]));
            }
            if (keyParts.length==1) {
                this.setJobKey(new JobKey(keyParts[0]));
            }
        }
        this.setState(TriggerState.valueOf(reader.readUTF("state")));
        
        
        
        byte[] triggerBytes = reader.readByteArray("trigger");
        ByteArrayInputStream bais = new ByteArrayInputStream(triggerBytes);
        
        Kryo kryo = new Kryo();
        Input input = new Input(bais);
        this.setTrigger(kryo.readObject(input, OperableTrigger.class));
        input.close();
    }

    @Override
    public void writePortable (PortableWriter writer) throws IOException {
          
        writer.writeLong("nextFireTime", getNextFireTime());
        writer.writeLong("acquiredAt", getAcquiredAt());
        writer.writeUTF("key", key.toString());
        writer.writeUTF("jobKey", jobKey.toString());
        writer.writeUTF("state", state.name());
        
        Kryo kryo = new Kryo();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, trigger);
        output.close();
        writer.writeByteArray("trigger", bos.toByteArray());

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

    public TriggerKey getKey () {
        return key;
    }

    public void setKey (TriggerKey key) {
        this.key = key;
    }

    public JobKey getJobKey () {
        return jobKey;
    }

    public void setJobKey (JobKey jobKey) {
        this.jobKey = jobKey;
    }
    
    public void setTrigger (OperableTrigger trigger) {
        this.trigger = trigger;
    }

    public void setAcquiredAt (Long acquiredAt) {
        this.acquiredAt = acquiredAt;
    }

    public void setState (TriggerState state) {
        this.state = state;
    }

}
