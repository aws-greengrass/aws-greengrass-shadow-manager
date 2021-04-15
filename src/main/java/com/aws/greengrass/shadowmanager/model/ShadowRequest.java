/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.util.Utils;
import lombok.Getter;

import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_TOPIC_PREFIX;
import static com.aws.greengrass.shadowmanager.model.Constants.NAMED_SHADOW_TOPIC_PREFIX;

/**
 * Internal class containing Shadow request information.
 */
@Getter
public class ShadowRequest {
    String thingName;
    String shadowName;

    /**
     * ShadowRequest constructor.
     *
     * @param thingName the thing name of the shadow request
     * @param shadowName the shadow name of the shadow request
     */
    public ShadowRequest(String thingName, String shadowName) {
        this.thingName = thingName;
        this.shadowName = Optional.ofNullable(shadowName)
                .filter(s -> !s.isEmpty())
                .orElse(CLASSIC_SHADOW_IDENTIFIER);
    }

    /**
     * ShadowRequest constructor for classic shadows.
     *
     * @param thingName the thing name of the shadow request
     */
    public ShadowRequest(String thingName) {
        this.thingName = thingName;
        this.shadowName = CLASSIC_SHADOW_IDENTIFIER;
    }

    /**
     * Forms either the classic or named shadow topic prefix.
     *
     * @return The shadow topic prefix
     */
    public String getShadowTopicPrefix() {
        if (Utils.isEmpty(shadowName)) {
            return String.format(CLASSIC_SHADOW_TOPIC_PREFIX, thingName);
        }
        return String.format(NAMED_SHADOW_TOPIC_PREFIX, thingName, shadowName);
    }

    /**
     * Forms either the classic or named shadow topic prefix.
     *
     * @return The shadow topic prefix
     */
    public String computeShadowLockKey() {
        return String.format("%s|%s", thingName, shadowName);
    }

}
