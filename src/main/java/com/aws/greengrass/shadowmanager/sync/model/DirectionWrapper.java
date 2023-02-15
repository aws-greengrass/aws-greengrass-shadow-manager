/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.function.Supplier;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DirectionWrapper implements Supplier<Direction> {

    private volatile Direction direction = Direction.BETWEEN_DEVICE_AND_CLOUD;

    @Override
    public Direction get() {
        return direction;
    }
}
