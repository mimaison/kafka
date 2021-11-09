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
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.metadata.MetadataRecordType.CONFIG_RECORD;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class TagsImageTest {
    final static TagsImage IMAGE1;

    final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static TagsDelta DELTA1;

    final static TagsImage IMAGE2;

    static {
        Map<TagResource, TagImage> map1 = new HashMap<>();
        Map<String, String> topic0Map = new HashMap<>();
        topic0Map.put("foo", "bar");
        topic0Map.put("baz", "quux");
        map1.put(new TagResource(TagResource.Type.TOPIC, "topic0"),
            new TagImage(topic0Map));
        Map<String, String> topic1Map = new HashMap<>();
        topic1Map.put("foobar", "foobaz");
        map1.put(new TagResource(TagResource.Type.TOPIC, "topic1"),
            new TagImage(topic1Map));
        IMAGE1 = new TagsImage(map1);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(TOPIC.id()).
            setResourceName("topic0").setName("foo").setValue(null),
            CONFIG_RECORD.highestSupportedVersion()));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new ConfigRecord().setResourceType(TOPIC.id()).
            setResourceName("topic1").setName("barfoo").setValue("bazfoo"),
            CONFIG_RECORD.highestSupportedVersion()));

        DELTA1 = new TagsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<TagResource, TagImage> map2 = new HashMap<>();
        Map<String, String> topic0Map2 = new HashMap<>();
        topic0Map2.put("baz", "quux");
        map2.put(new TagResource(TagResource.Type.TOPIC, "topic0"),
            new TagImage(topic0Map2));
        Map<String, String> topic1Map2 = new HashMap<>();
        topic1Map2.put("foobar", "foobaz");
        topic1Map2.put("barfoo", "bazfoo");
        map2.put(new TagResource(TagResource.Type.TOPIC, "topic1"),
            new TagImage(topic1Map2));
        IMAGE2 = new TagsImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImageAndBack(TagsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() throws Throwable {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    @Test
    public void testImage2RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(TagsImage image) throws Throwable {
        MockSnapshotConsumer writer = new MockSnapshotConsumer();
        image.write(writer);
        TagsDelta delta = new TagsDelta(TagsImage.EMPTY);
        RecordTestUtils.replayAllBatches(delta, writer.batches());
        TagsImage nextImage = delta.apply();
        assertEquals(image, nextImage);
    }
}
