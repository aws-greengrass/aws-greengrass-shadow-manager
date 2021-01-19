package com.aws.greengrass.shadowmanager;

import java.util.Optional;

public interface ShadowManagerDAO {
    /**
     * Attempts to create a shadow document from the local shadow storage.
     * @param thingName The thing namespace of the shadow document.
     * @param initialDocument The initial shadow document.
     * @return Optional
     */
    Optional<byte[]> createShadowThing(String thingName, byte[] initialDocument);

    /**
     * Attempts to obtain a shadow document from the local shadow storage.
     * @param thingName The thing namespace of the shadow document.
     * @return Optional
     */
    Optional<byte[]> getShadowThing(String thingName);

    /**
     * Attempts to delete a shadow document from the local shadow storage.
     * @param thingName The thing namespace of the shadow document.
     * @return Optional
     */
    Optional<byte[]> deleteShadowThing(String thingName);

    /**
     * Attempts to update a shadow document from the local shadow storage.
     * @param thingName The thing namespace of the shadow document.
     * @param newDocument The new shadow document.
     * @return Optional
     */
    Optional<byte[]> updateShadowThing(String thingName, byte[] newDocument);
}
