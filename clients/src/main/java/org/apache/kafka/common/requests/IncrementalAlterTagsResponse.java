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

import org.apache.kafka.common.TagResource;
import org.apache.kafka.common.message.IncrementalAlterTagsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IncrementalAlterTagsResponse extends AbstractResponse {

    public IncrementalAlterTagsResponse(final int requestThrottleMs,
                                        final Map<TagResource, ApiError> results) {
        super(ApiKeys.INCREMENTAL_ALTER_TAGS);
        final List<IncrementalAlterTagsResponseData.AlterTagsResourceResponse> newResults = new ArrayList<>(results.size());
        results.forEach(
            (resource, error) -> newResults.add(
                new IncrementalAlterTagsResponseData.AlterTagsResourceResponse()
                    .setErrorCode(error.error().code())
                    .setErrorMessage(error.message())
                    .setResourceName(resource.name())
                    .setResourceType(resource.type().id()))
        );

        this.data = new IncrementalAlterTagsResponseData()
                        .setResponses(newResults)
                        .setThrottleTimeMs(requestThrottleMs);
    }

    public static Map<TagResource, ApiError> fromResponseData(final IncrementalAlterTagsResponseData data) {
        Map<TagResource, ApiError> map = new HashMap<>();
        for (IncrementalAlterTagsResponseData.AlterTagsResourceResponse response : data.responses()) {
            map.put(new TagResource(TagResource.Type.forId(response.resourceType()), response.resourceName()),
                    new ApiError(Errors.forCode(response.errorCode()), response.errorMessage()));
        }
        return map;
    }

    private final IncrementalAlterTagsResponseData data;

    public IncrementalAlterTagsResponse(IncrementalAlterTagsResponseData data) {
        super(ApiKeys.INCREMENTAL_ALTER_TAGS);
        this.data = data;
    }

    @Override
    public IncrementalAlterTagsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        data.responses().forEach(response ->
            updateErrorCounts(counts, Errors.forCode(response.errorCode()))
        );
        return counts;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 0;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public static IncrementalAlterTagsResponse parse(ByteBuffer buffer, short version) {
        return new IncrementalAlterTagsResponse(new IncrementalAlterTagsResponseData(
            new ByteBufferAccessor(buffer), version));
    }
}
