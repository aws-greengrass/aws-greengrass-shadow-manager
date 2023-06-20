/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public final class ShadowSyncConfigurationUtils {
    public static ShadowSyncConfiguration syncConfig(boolean addOnInteractionEnabled, Object... syncs) {
        AddOnInteractionSyncConfiguration addOnInteraction = AddOnInteractionSyncConfiguration
            .builder()
            .enabled(addOnInteractionEnabled)
            .build();



        return ShadowSyncConfiguration.builder()
            .syncConfigurations(syncConfigs(syncs))
            .addOnInteraction(addOnInteraction)
            .build();
    }

    public static Set<ThingShadowSyncConfiguration> syncConfigs(Object... syncs) {
        if (syncs.length % 3 != 0) {
            return fail("syncs are wrong");
        }
        Set<ThingShadowSyncConfiguration> syncConfigurations = new HashSet<>();
        for (int i = 0; i < syncs.length; i += 3) {
            syncConfigurations.add(ThingShadowSyncConfiguration.builder()
                .thingName((String)syncs[i])
                .shadowName((String)syncs[i+1])
                .addedOnInteraction((boolean)syncs[i+2])
                .build()
            );
        }

        return syncConfigurations;
    }

    public static void assertEqual(
        Set<ThingShadowSyncConfiguration> actual,
        Set<ThingShadowSyncConfiguration> expected
    ) {
        List<ThingShadowSyncConfiguration> actualList = new ArrayList<>(actual);
        List<ThingShadowSyncConfiguration> expectedList = new ArrayList<>(expected);
        assertEquals(actualList.size(), expectedList.size());
        assertThat(actualList, Matchers.containsInAnyOrder(expectedList.toArray()));
    }

    public static void assertEqual(
        ShadowSyncConfiguration actual,
        ShadowSyncConfiguration expected
    ) {
        assertThat(actual, is(equalTo(expected)));
        assertEqual(actual.getSyncConfigurations(), expected.getSyncConfigurations());
    }

    private ShadowSyncConfigurationUtils() {
    }
}
