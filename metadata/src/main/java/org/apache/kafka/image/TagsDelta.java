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

package org.apache.kafka.image;

import org.apache.kafka.common.TagResource;
import org.apache.kafka.common.TagResource.Type;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TagRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Represents changes to the tags in the metadata image.
 */
public final class TagsDelta {
    private final TagsImage image;
    private final Map<TagResource, TagDelta> changes = new HashMap<>();

    public TagsDelta(TagsImage image) {
        this.image = image;
    }

    public Map<TagResource, TagDelta> changes() {
        return changes;
    }

    public void finishSnapshot() {
        for (Entry<TagResource, TagImage> entry : image.resourceData().entrySet()) {
            TagResource resource = entry.getKey();
            TagImage tagImage = entry.getValue();
            TagDelta tagDelta = changes.computeIfAbsent(resource,
                __ -> new TagDelta(tagImage));
            tagDelta.finishSnapshot();
        }
    }

    public void replay(TagRecord record) {
        TagResource resource =
                new TagResource(Type.forId(record.resourceType()), record.resourceName());
        TagImage tagImage =
            image.resourceData().getOrDefault(resource, TagImage.EMPTY);
        TagDelta delta = changes.computeIfAbsent(resource,
            __ -> new TagDelta(tagImage));
        delta.replay(record);
    }

    public void replay(RemoveTopicRecord record, String topicName) {
        TagResource resource =
                new TagResource(TagResource.Type.TOPIC, topicName);
        TagImage tagImage =
                image.resourceData().getOrDefault(resource, TagImage.EMPTY);
        TagDelta delta = changes.computeIfAbsent(resource,
                __ -> new TagDelta(tagImage));
        delta.deleteAll();
    }

    public TagsImage apply() {
        Map<TagResource, TagImage> newData = new HashMap<>();
        for (Entry<TagResource, TagImage> entry : image.resourceData().entrySet()) {
            TagResource resource = entry.getKey();
            TagDelta delta = changes.get(resource);
            if (delta == null) {
                newData.put(resource, entry.getValue());
            } else {
                TagImage newImage = delta.apply();
                if (!newImage.isEmpty()) {
                    newData.put(resource, newImage);
                }
            }
        }
        for (Entry<TagResource, TagDelta> entry : changes.entrySet()) {
            if (!newData.containsKey(entry.getKey())) {
                TagImage newImage = entry.getValue().apply();
                if (!newImage.isEmpty()) {
                    newData.put(entry.getKey(), newImage);
                }
            }
        }
        return new TagsImage(newData);
    }

    @Override
    public String toString() {
        return "TagsDelta(" +
            "changes=" + changes +
            ')';
    }
}
