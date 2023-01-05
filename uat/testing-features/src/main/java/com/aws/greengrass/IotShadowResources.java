/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.resources.iot.IotCertificate;
import com.aws.greengrass.testing.resources.iot.IotPolicy;
import com.aws.greengrass.testing.resources.iot.IotRoleAlias;
import com.aws.greengrass.testing.resources.iot.IotThing;
import com.aws.greengrass.testing.resources.iot.IotThingGroup;
import com.aws.greengrass.testing.resources.s3.S3Bucket;
import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iot.IotClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.inject.Inject;

@Getter
@Log4j2
public class IotShadowResources implements Closeable {
    protected static int MAX_RETRY_NUM = 5;
    protected static int INITIAL_RETRY_INTERVAL_SEC = 2;
    protected static int MAX_RETRY_INTERVAL_SEC = 30;

    private final IotShadowResourceSpec specs;
    private final Resources resources;
    private final IotClient iotClient;
    private final IamClient iamClient;

    /**
     * constructor for IotShadowResources.
     *
     * @param iotClient iotClient instance
     * @param iamClient iamClient instance
     */
    @Inject
    public IotShadowResources(IotClient iotClient, IamClient iamClient) {
        this.specs = IotShadowResourceSpec.builder().build();
        this.iamClient = iamClient;
        this.iotClient = iotClient;
        this.resources = new Resources(this);
    }

    @Override
    @ResourceRelease
    public void close() throws IOException {
        resources.close();
    }

    @Data
    public static class Resources implements Closeable {
        IotClient iotClient;
        IamClient iamClient;

        public Resources(final IotShadowResources resources) {
            this.iotClient = resources.iotClient;
            this.iamClient = resources.iamClient;
        }

        List<IotThing> things = new ArrayList<>();
        List<IotThingGroup> rootThingGroups = new ArrayList<>();   // group hierarchy for creation and removal
        List<IotThingGroup> thingGroups = new ArrayList<>();  // flat list of all groups for easy lookup
        List<IotPolicy> policies = new ArrayList<>();
        List<IotRoleAlias> roleAliases = new ArrayList<>();
        List<IotCertificate> certificates = new ArrayList<>();
        List<IoTShadow> ioTShadows = new ArrayList<>();
        List<S3Bucket> s3Buckets = new ArrayList<>();

        @Override
        public void close() throws IOException {
            log.atInfo().log("Removing all AWS resources");

            final AtomicReference<Throwable> ex = new AtomicReference<>(null);
            Consumer<Throwable> updateEx = (e) -> {
                if (ex.get() == null) {
                    ex.set(e);
                } else {
                    ex.get().addSuppressed(e);
                }
                log.atError().log("Exception while removing resource", e);
            };

            for (IotThing thing : things) {
                try {
                    thing.remove(iotClient);
                } catch (Throwable e) {
                    updateEx.accept(e);
                }
            }

            for (IotThingGroup group : rootThingGroups) {
                try {
                    group.remove(iotClient);
                } catch (Throwable e) {
                    updateEx.accept(e);
                }
            }

            for (IotCertificate certificate : certificates) {
                try {
                    certificate.remove(iotClient);
                } catch (Throwable e) {
                    updateEx.accept(e);
                }
            }

            if (ex.get() != null) {
                throw new IOException(ex.get());
            }
        }
    }
}
