/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.artifacts;

import java.util.function.Consumer;

public class Hello implements Consumer<String[]> {
    @Override
    public void accept(String[] strings) {
        System.out.println("This is just a placeholder");
    }
}
