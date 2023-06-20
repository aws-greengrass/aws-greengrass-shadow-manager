/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.config.Node;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import lombok.Setter;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_ADDED_ON_INTERACTION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;

public class SyncConfigurationUpdater {
    private static final Logger logger = LogManager.getLogger(SyncConfigurationUpdater.class);

    @Setter
    private Topics topics;

    @Inject
    public SyncConfigurationUpdater() {

    }

    /**
     * Updates ShadowManager component configuration with the shadows added on interaction.
     * Does not touch the configuration for non-added-on-interaction shadows.
     *
     * @param configurations - The things shadows to be synchronized.
     */
    public void updateThingShadowsAddedOnInteraction(Set<ThingShadowSyncConfiguration> configurations) {
        Map<String, Map<String, Boolean>> configsMap = configurations.stream()
            // filter out configurations that were not added on interaction
            .filter(ThingShadowSyncConfiguration::isAddedOnInteraction)

            // group them by the thing name and make a nested map from each group
            // thing1:
            //   shadow1: true
            //   shadow2: true
            // thing2:
            //   shadow3: true
            //   shadow4: true
            .collect(Collectors.groupingBy(ThingShadowSyncConfiguration::getThingName, Collectors.toMap(
                ThingShadowSyncConfiguration::getShadowName,
                x -> true
            )));

        Map<String, Object> valueToMerge = (Map)configsMap;

        Node node = topics.findNode(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC,
            CONFIGURATION_ADDED_ON_INTERACTION_TOPIC);
        long updateTime = Instant.now().toEpochMilli();
        if (node == null) {
            logger.atInfo().kv("valueToMerge", valueToMerge).log("node does not exist, creating one");
            Topics syncTopics = topics.lookupTopics(CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC,
                CONFIGURATION_ADDED_ON_INTERACTION_TOPIC);
            updateTime = topics.getModtime();
            syncTopics.updateFromMap(valueToMerge,
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, updateTime));
            logger.atInfo().log("node does not exist, updated config");
            return;
        }

        if (node instanceof Topic) {
            logger.atInfo().kv("valueToMerge", valueToMerge).kv("node", node).log("node is a topic");
            Topic topic = (Topic) node;
            topic.parent.updateFromMap(Collections.singletonMap(topic.getName(), valueToMerge),
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, updateTime));
            logger.atInfo().log("node is a topic, updated config");
            return;
        }

        logger.atInfo().kv("valueToMerge", valueToMerge).kv("node", node).log("node is topics");
        Topics syncTopics = (Topics) node;
        syncTopics.updateFromMap(valueToMerge,
            new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, updateTime));
        logger.atInfo().log("node is topics, updated config");
    }
}
