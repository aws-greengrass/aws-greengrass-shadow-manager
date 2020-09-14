package com.aws.iot.greengrass.shadowmanager;

import java.nio.ByteBuffer;
import java.util.Optional;

public interface ShadowManagerDAO {
    /**
     * Attempts to obtain a shadow document from the local shadow storage.
     * @param thingName The thing namespace of the shadow document.
     * @return Optional
     */
    Optional<ByteBuffer> getShadowThing(String thingName);
}
