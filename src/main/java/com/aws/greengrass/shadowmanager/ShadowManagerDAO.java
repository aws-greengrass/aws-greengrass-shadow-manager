/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.util.Pair;

import java.util.List;
import java.util.Optional;

public interface ShadowManagerDAO {
    /**
     * Attempts to obtain a shadow document from the local shadow storage.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return The queried shadow from the local shadow store
     */
    Optional<ShadowDocument> getShadowThing(String thingName, String shadowName);

    /**
     * Attempts to delete a shadow document from the local shadow storage.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return The deleted shadow from the local shadow store
     */
    Optional<ShadowDocument> deleteShadowThing(String thingName, String shadowName);

    /**
     * Attempts to update a shadow document from the local shadow storage.
     *
     * @param thingName   Name of the Thing for the shadow topic prefix.
     * @param shadowName  Name of shadow topic prefix for thing.
     * @param newDocument The new shadow document.
     * @param version     The new version of the shadow document.
     * @return Optional containing the new shadow document if update is successful; Else an empty optional
     */
    Optional<byte[]> updateShadowThing(String thingName, String shadowName, byte[] newDocument, long version);

    /**
     * Attempts to retrieve list of named shadows for a specified thing from the local shadow storage.
     *
     * @param thingName Name of the Thing to check Named Shadows.
     * @param offset    Number of Named Shadows to bypass.
     * @param limit     Maximum number of Named Shadows to retrieve.
     * @return A limited list of named shadows matching the specified thingName
     */
    List<String> listNamedShadowsForThing(String thingName, int offset, int limit);

    /**
     * Attempts to update the sync information for a particular thing's shadow.
     *
     * @param request The update shadow sync information request containing the necessary information to update.
     * @return true if the update is successful; Else false.
     */
    boolean updateSyncInformation(SyncInformation request);

    /**
     * Attempts to obtain a shadow sync information for a particular thing's shadow.
     *
     * @param thingName  Name of the Thing for the shadow topic prefix.
     * @param shadowName Name of shadow topic prefix for thing.
     * @return The queried shadow sync information from the local shadow store
     */
    Optional<SyncInformation> getShadowSyncInformation(String thingName, String shadowName);

    /**
     * Attempts to obtain a list of all synced shadow names.
     *
     * @return The queried synced shadow names list.
     */
    List<Pair<String, String>> listSyncedShadows();

    /**
     * Attempts to delete the cloud shadow document in the sync table.
     *
     * @param thingName       Name of the Thing for the shadow topic prefix.
     * @param shadowName      Name of shadow topic prefix for thing.
     * @return true if the cloud document (soft) delete was successful or not.
     */
    boolean deleteSyncInformation(String thingName, String shadowName);
}