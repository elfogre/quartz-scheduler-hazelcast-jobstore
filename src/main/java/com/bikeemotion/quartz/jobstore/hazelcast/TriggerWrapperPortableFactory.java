package com.bikeemotion.quartz.jobstore.hazelcast;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class TriggerWrapperPortableFactory implements PortableFactory {

    @Override
    public Portable create( int classId ) {
      if ( TriggerWrapper.ID == classId )
        return new TriggerWrapper();
      else
        return null;
    }
}
