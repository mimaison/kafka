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

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.TagResource;
import org.apache.kafka.common.message.IncrementalAlterTagsRequestData;
import org.apache.kafka.common.message.IncrementalAlterTagsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

public class IncrementalAlterTagsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<IncrementalAlterTagsRequest> {
        private final IncrementalAlterTagsRequestData data;

        public Builder(IncrementalAlterTagsRequestData data) {
            super(ApiKeys.INCREMENTAL_ALTER_TAGS);
            this.data = data;
        }

        public Builder(final Collection<TagResource> resources,
                       final Map<TagResource, Collection<AlterConfigOp>> configs) {
            super(ApiKeys.INCREMENTAL_ALTER_TAGS);
            this.data = new IncrementalAlterTagsRequestData();
            for (TagResource resource : resources) {
                IncrementalAlterTagsRequestData.AlterableTagCollection alterableConfigSet =
                    new IncrementalAlterTagsRequestData.AlterableTagCollection();
                for (AlterConfigOp configEntry : configs.get(resource))
                    alterableConfigSet.add(new IncrementalAlterTagsRequestData.AlterableTag()
                                               .setName(configEntry.configEntry().name())
                                               .setValue(configEntry.configEntry().value())
                                               .setTagOperation(configEntry.opType().id()));
                IncrementalAlterTagsRequestData.AlterTagsResource alterConfigsResource = new IncrementalAlterTagsRequestData.AlterTagsResource();
                alterConfigsResource.setResourceType(resource.type().id())
                    .setResourceName(resource.name()).setTags(alterableConfigSet);
                data.resources().add(alterConfigsResource);
            }
        }

        @Override
        public IncrementalAlterTagsRequest build(short version) {
            return new IncrementalAlterTagsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final IncrementalAlterTagsRequestData data;
    private final short version;

    private IncrementalAlterTagsRequest(IncrementalAlterTagsRequestData data, short version) {
        super(ApiKeys.INCREMENTAL_ALTER_TAGS, version);
        this.data = data;
        this.version = version;
    }

    public static IncrementalAlterTagsRequest parse(ByteBuffer buffer, short version) {
        return new IncrementalAlterTagsRequest(new IncrementalAlterTagsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public IncrementalAlterTagsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(final int throttleTimeMs, final Throwable e) {
        IncrementalAlterTagsResponseData response = new IncrementalAlterTagsResponseData();
        ApiError apiError = ApiError.fromThrowable(e);
        for (IncrementalAlterTagsRequestData.AlterTagsResource resource : data.resources()) {
            response.responses().add(new IncrementalAlterTagsResponseData.AlterTagsResourceResponse()
                    .setResourceName(resource.resourceName())
                    .setResourceType(resource.resourceType())
                    .setErrorCode(apiError.error().code())
                    .setErrorMessage(apiError.message()));
        }
        return new IncrementalAlterTagsResponse(response);
    }
}
