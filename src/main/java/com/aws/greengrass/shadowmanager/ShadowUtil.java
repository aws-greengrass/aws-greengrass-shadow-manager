/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.util.Utils;

import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_TOPIC_PREFIX;
import static com.aws.greengrass.shadowmanager.model.Constants.NAMED_SHADOW_TOPIC_PREFIX;

public final class ShadowUtil {

    /**
     * Forms either the classic or named shadow topic prefix.
     *
     * @param thingName The name of the shadow
     * @param shadowName The name of the shadow
     * @return The shadow topic prefix
     */
    public static String getShadowTopicPrefix(String thingName, String shadowName) {
        if (Utils.isEmpty(shadowName)) {
            return String.format(CLASSIC_SHADOW_TOPIC_PREFIX, thingName);
        }
        return String.format(NAMED_SHADOW_TOPIC_PREFIX, thingName, shadowName);
    }

    /**
     * Checks and returns the CLASSIC_SHADOW_IDENTIFIER if shadowName input was null.
     *
     * @param shadowName The name of the shadow
     * @return The shadowName input or CLASSIC_SHADOW_IDENTIFIER
     */
    public static String getClassicShadowIfMissingShadowName(String shadowName) {
        return Optional.ofNullable(shadowName)
                .filter(s -> !s.isEmpty())
                .orElse(CLASSIC_SHADOW_IDENTIFIER);
    }

    private ShadowUtil() {

    }

}
