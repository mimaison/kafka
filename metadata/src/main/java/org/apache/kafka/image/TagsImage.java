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

import org.apache.kafka.clients.admin.TagPredicate;
import org.apache.kafka.common.TagResource;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Represents the tags in the metadata image.
 *
 * This class is thread-safe.
 */
public final class TagsImage {
    public static final TagsImage EMPTY =
        new TagsImage(Collections.emptyMap());

    private final Map<TagResource, TagImage> data;
    private final Map<String, List<TagResource>> byTag = new HashMap<>();

    Logger log = LoggerFactory.getLogger(TagsImage.class.getName());

    public TagsImage(Map<TagResource, TagImage> data) {
        this.data = Collections.unmodifiableMap(data);
        for (Map.Entry<TagResource, TagImage> entry : data.entrySet()) {
            for (Entry<String, String> tag : entry.getValue().data().entrySet()) {
                byTag.computeIfAbsent(tag.getKey(), k -> new ArrayList<>()).add(entry.getKey());
            }
        }
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    Map<TagResource, TagImage> resourceData() {
        return data;
    }

    public Properties tagsProperties(TagResource tagResource) {
        TagImage tagImage = data.get(tagResource);
        if (tagImage != null) {
            return tagImage.toProperties();
        } else {
            return new Properties();
        }
    }

    public Set<String> query(MetadataRequestData.TopicTagCollection predicates) {
        Set<String> result = new HashSet<>();
        for (MetadataRequestData.TopicTag predicate : predicates) {
            log.info("checking predicate {} {} {}", predicate.op(), predicate.key(), predicate.values());
            List<TagResource> found = byTag.get(predicate.key());
            if (found == null || found.isEmpty()) continue;
            if (predicate.op() == TagPredicate.OpType.HAS.id()) {
                return found.stream().map(TagResource::name).collect(Collectors.toSet());
            }
            for (TagResource tr : found) {
                Properties tags = tagsProperties(tr);
                String val = tags.getProperty(predicate.key());
                switch (TagPredicate.OpType.forId(predicate.op())) {
                    case EQUAL:
                        String p = predicate.values().get(0);
                        if (p.equals(val)) {
                            result.add(tr.name());
                        }
                        break;
                    case NOT_EQUAL:
                        String q = predicate.values().get(0);
                        if (!q.equals(val)) {
                            result.add(tr.name());
                        }
                        break;
                    case IN:
                        if (predicate.values().contains(val)) {
                            result.add(tr.name());
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("oops");
                }
            }
        }
        return result;
    }

    public void write(Consumer<List<ApiMessageAndVersion>> out) {
        for (Entry<TagResource, TagImage> entry : data.entrySet()) {
            TagResource tagResource = entry.getKey();
            TagImage tagImage = entry.getValue();
            tagImage.write(tagResource, out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TagsImage)) return false;
        TagsImage other = (TagsImage) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() {
        return "TagsImage(data=" + data.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
            ")";
    }
}
