/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.TagResource;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.TagRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static org.apache.kafka.common.metadata.MetadataRecordType.TAG_RECORD;


public class TagControlManager {
    private final Logger log;
    private final SnapshotRegistry snapshotRegistry;
    private final TimelineHashMap<TagResource, TimelineHashMap<String, String>> tagData;

    TagControlManager(LogContext logContext, SnapshotRegistry snapshotRegistry) {
        this.log = logContext.logger(TagControlManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.tagData = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    ControllerResult<Map<String, ApiError>> incrementalAlterTags(
            Map<TagResource, Map<String, Entry<OpType, String>>> tagChanges) {
        List<ApiMessageAndVersion> outputRecords = new ArrayList<>();
        Map<String, ApiError> outputResults = new HashMap<>();
        for (Entry<TagResource, Map<String, Entry<OpType, String>>> resourceEntry :
                tagChanges.entrySet()) {
            incrementalAlterTagResource(resourceEntry.getKey(),
                resourceEntry.getValue(),
                outputRecords,
                outputResults);
        }
        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    private void incrementalAlterTagResource(TagResource tagResource,
                                                Map<String, Entry<OpType, String>> keysToOps,
                                                List<ApiMessageAndVersion> outputRecords,
                                                Map<String, ApiError> outputResults) {
        //TODO
        List<ApiMessageAndVersion> newRecords = new ArrayList<>();
        for (Entry<String, Entry<OpType, String>> keysToOpsEntry : keysToOps.entrySet()) {
            String key = keysToOpsEntry.getKey();
            String currentValue = null;
            TimelineHashMap<String, String> currentConfigs = tagData.get(tagResource);
            if (currentConfigs != null) {
                currentValue = currentConfigs.get(key);
            }
            String newValue = currentValue;
            Entry<OpType, String> opTypeAndNewValue = keysToOpsEntry.getValue();
            OpType opType = opTypeAndNewValue.getKey();
            String opValue = opTypeAndNewValue.getValue();
            switch (opType) {
                case SET:
                    newValue = opValue;
                    break;
                default:
                    throw new IllegalStateException("oops");
            }
            if (!Objects.equals(currentValue, newValue)) {
                newRecords.add(new ApiMessageAndVersion(new TagRecord()
                        .setResourceType(tagResource.type().id())
                        .setResourceName(tagResource.name())
                        .setName(key)
                        .setValue(newValue), TAG_RECORD.highestSupportedVersion()));
            }
        }

        outputRecords.addAll(newRecords);
        outputResults.put(tagResource.name(), ApiError.NONE);
    }

    /**
     * Apply a tag record to the in-memory state.
     *
     * @param record            The TagRecord.
     */
    public void replay(TagRecord record) {
        TagResource.Type type = TagResource.Type.forId(record.resourceType());
        TagResource tagResource = new TagResource(type, record.resourceName());
        TimelineHashMap<String, String> tags = tagData.get(tagResource);
        if (tags == null) {
            tags = new TimelineHashMap<>(snapshotRegistry, 0);
            tagData.put(tagResource, tags);
        }
        if (record.value() == null) {
            tags.remove(record.name());
        } else {
            tags.put(record.name(), record.value());
        }
        if (tags.isEmpty()) {
            tagData.remove(tagResource);
        }
        log.info("{}: set tag {} to {}", tagResource, record.name(), record.value());
    }

    void deleteTopicTags(String name) {
        tagData.remove(new TagResource(TagResource.Type.TOPIC, name));
    }
}
