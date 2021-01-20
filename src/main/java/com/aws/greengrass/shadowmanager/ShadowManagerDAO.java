package com.aws.greengrass.shadowmanager;

import java.util.Optional;

public interface ShadowManagerDAO {
    /**
     * Attempts to create a shadow document from the local shadow storage.
     * @param thingName Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @param initialDocument The initial shadow document.
     * @return Optional
     */
    Optional<byte[]> createShadowThing(String thingName, String shadowName ,byte[] initialDocument);

    /**
     * Attempts to obtain a shadow document from the local shadow storage.
     * @param thingName Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return Optional
     */
    Optional<byte[]> getShadowThing(String thingName, String shadowName);

    /**
     * Attempts to delete a shadow document from the local shadow storage.
     * @param thingName Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return Optional
     */
    Optional<byte[]> deleteShadowThing(String thingName, String shadowName);

    /**
     * Attempts to update a shadow document from the local shadow storage.
     * @param thingName Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @param newDocument The new shadow document.
     * @return Optional
     */
    Optional<byte[]> updateShadowThing(String thingName, String shadowName, byte[] newDocument);
}
