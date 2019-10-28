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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData.CreatableAcl;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateAclsResponseData.CreatableAclResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public class CreateAclsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<CreateAclsRequest> {
        private final CreateAclsRequestData data;

        public Builder(Collection<AclBinding> acls) {
            super(ApiKeys.CREATE_ACLS);
            List<CreatableAcl> creatableAcls = new ArrayList<>();
            for (AclBinding binding : acls) {
                AccessControlEntry entry = binding.entry();
                ResourcePattern pattern = binding.pattern();
                CreatableAcl acl = new CreatableAcl()
                        .setHost(entry.host())
                        .setOperation(entry.operation().code())
                        .setPermissionType(entry.permissionType().code())
                        .setPrincipal(entry.principal())
                        .setResourceName(pattern.name())
                        .setResourcePatternType(pattern.patternType().code())
                        .setResourceType(pattern.resourceType().code());
                creatableAcls.add(acl);
            }
            this.data = new CreateAclsRequestData().setCreations(creatableAcls);
        }

        public Builder(CreateAclsRequestData data) {
            super(ApiKeys.CREATE_ACLS);
            this.data = data;
        }

        @Override
        public CreateAclsRequest build(short version) {
            CreateAclsRequest request = new CreateAclsRequest(version, data);
            validate(version);
            return request;
        }

        @Override
        public String toString() {
            return data.toString();
        }

        private void validate(short version) {
            if (version == 0) {
                final boolean unsupported =  data.creations().stream()
                        .anyMatch(creation -> creation.resourcePatternType() != PatternType.LITERAL.code());
                if (unsupported) {
                    throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
                }
            }

            final boolean unknown = data.creations().stream().anyMatch(creation ->
                    creation.resourceType() == ResourceType.UNKNOWN.code()
                    || creation.resourcePatternType() == PatternType.UNKNOWN.code()
                    || creation.operation() == AclOperation.UNKNOWN.code()
                    || creation.permissionType() == AclPermissionType.UNKNOWN.code());
            if (unknown) {
                throw new IllegalArgumentException("You can not create ACL bindings with unknown elements");
            }
        }

    }

    private final CreateAclsRequestData data;

    CreateAclsRequest(short version, CreateAclsRequestData data) {
        super(ApiKeys.CREATE_ACLS, version);
        this.data = data;
    }

    public CreateAclsRequest(Struct struct, short version) {
        super(ApiKeys.CREATE_ACLS, version);
        this.data = new CreateAclsRequestData(struct, version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                ApiError error = ApiError.fromThrowable(throwable);
                List<CreatableAclResult> results = data.creations()
                       .stream().map(acl -> new CreatableAclResult()
                            .setErrorCode(error.error().code())
                            .setErrorMessage(error.message())).collect(Collectors.toList());

                CreateAclsResponseData data = new CreateAclsResponseData()
                        .setThrottleTimeMs(throttleTimeMs)
                        .setResults(results);
                return new CreateAclsResponse(data);
            default:
                throw new IllegalArgumentException(
                        String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                                versionId,
                                this.getClass().getSimpleName(),
                                ApiKeys.CREATE_ACLS.latestVersion()));
        }
    }

    public CreateAclsRequestData data() {
        return data;
    }

    public List<AclBinding> aclBindings() {
        List<AclBinding> bindings = new ArrayList<>();
        for (CreatableAcl acl : data.creations()) {
            ResourcePattern pattern = new ResourcePattern(
                    ResourceType.fromCode(acl.resourceType()),
                    acl.resourceName(),
                    PatternType.fromCode(acl.resourceType()));
            AccessControlEntry entry = new AccessControlEntry(
                    acl.principal(),
                    acl.host(),
                    AclOperation.fromCode(acl.operation()),
                    AclPermissionType.fromCode(acl.permissionType()));
            AclBinding binding = new AclBinding(pattern, entry);
            bindings.add(binding);
        }
        return bindings;
    }

    public static CreateAclsRequest parse(ByteBuffer buffer, short version) {
        return new CreateAclsRequest(ApiKeys.CREATE_ACLS.parseRequest(version, buffer), version);
    }

}
