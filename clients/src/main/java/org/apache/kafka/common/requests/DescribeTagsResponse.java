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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DescribeTagsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DescribeTagsResponse extends AbstractResponse {

    public static class Tag {
        private final ApiError error;
        private final Collection<TagEntry> entries;

        public Tag(ApiError error, Collection<TagEntry> entries) {
            this.error = Objects.requireNonNull(error, "error");
            this.entries = Objects.requireNonNull(entries, "entries");
        }

        public ApiError error() {
            return error;
        }

        public Collection<TagEntry> entries() {
            return entries;
        }
    }

    public static class TagEntry {
        private final String name;
        private final String value;

        public TagEntry(String name, String value) {
            this.name = Objects.requireNonNull(name, "name");
            this.value = value;
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }
    }

    private final DescribeTagsResponseData data;

    public DescribeTagsResponse(DescribeTagsResponseData data) {
        super(ApiKeys.DESCRIBE_TAGS);
        this.data = data;
    }

    private DescribeTagsResponse(DescribeTagsResponseData data, short version) {
        super(ApiKeys.DESCRIBE_TAGS);
        this.data = data;
    }

    @Override
    public DescribeTagsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.results().forEach(response ->
            updateErrorCounts(errorCounts, Errors.forCode(response.errorCode()))
        );
        return errorCounts;
    }

    public static DescribeTagsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeTagsResponse(new DescribeTagsResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 0;
    }
}
