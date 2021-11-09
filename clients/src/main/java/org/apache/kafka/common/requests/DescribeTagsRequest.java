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

import org.apache.kafka.common.message.DescribeTagsRequestData;
import org.apache.kafka.common.message.DescribeTagsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

public class DescribeTagsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DescribeTagsRequest> {
        private final DescribeTagsRequestData data;

        public Builder(DescribeTagsRequestData data) {
            super(ApiKeys.DESCRIBE_TAGS);
            this.data = data;
        }

        @Override
        public DescribeTagsRequest build(short version) {
            return new DescribeTagsRequest(data, version);
        }
    }

    private final DescribeTagsRequestData data;

    public DescribeTagsRequest(DescribeTagsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_TAGS, version);
        this.data = data;
    }

    @Override
    public DescribeTagsRequestData data() {
        return data;
    }

    @Override
    public DescribeTagsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new DescribeTagsResponse(new DescribeTagsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setResults(data.resources().stream().map(result -> new DescribeTagsResponseData.DescribeTagsResult().setErrorCode(error.code())
                        .setErrorMessage(error.message())
                        .setResourceName(result.resourceName())
                        .setResourceType(result.resourceType())).collect(Collectors.toList())
        ));
    }

    public static DescribeTagsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeTagsRequest(new DescribeTagsRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
