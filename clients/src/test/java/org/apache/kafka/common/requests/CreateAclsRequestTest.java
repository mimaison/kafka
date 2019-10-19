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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.CreateAclsRequestData.CreatableAcl;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

public class CreateAclsRequestTest {
    private static final short V0 = 0;
    private static final short V1 = 1;

    private static final AclBinding LITERAL_ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    private static final AclBinding LITERAL_ACL2 = new AclBinding(new ResourcePattern(ResourceType.GROUP, "group", PatternType.LITERAL),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.WRITE, AclPermissionType.ALLOW));

    private static final AclBinding PREFIXED_ACL1 = new AclBinding(new ResourcePattern(ResourceType.GROUP, "prefix", PatternType.PREFIXED),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

    private static final AclBinding UNKNOWN_ACL1 = new AclBinding(new ResourcePattern(ResourceType.UNKNOWN, "unknown", PatternType.LITERAL),
        new AccessControlEntry("User:*", "127.0.0.1", AclOperation.CREATE, AclPermissionType.ALLOW));

    @Test(expected = UnsupportedVersionException.class)
    public void shouldThrowOnV0IfNotLiteral() {
        new CreateAclsRequest.Builder(Arrays.asList(PREFIXED_ACL1)).build(V0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIfUnknown() {
        new CreateAclsRequest.Builder(Arrays.asList(UNKNOWN_ACL1)).build(V0);
    }

    @Test
    public void shouldRoundTripV0() {
        final CreateAclsRequest original = new CreateAclsRequest.Builder(Arrays.asList(LITERAL_ACL1, LITERAL_ACL2)).build(V0);
        final Struct struct = original.toStruct();

        final CreateAclsRequest result = new CreateAclsRequest(struct, V0);

        assertRequestEquals(original, result);
    }

    @Test
    public void shouldRoundTripV1() {
        final CreateAclsRequest original = new CreateAclsRequest.Builder(Arrays.asList(LITERAL_ACL1, PREFIXED_ACL1)).build(V1);
        final Struct struct = original.toStruct();

        final CreateAclsRequest result = new CreateAclsRequest(struct, V1);

        assertRequestEquals(original, result);
    }

    private static void assertRequestEquals(final CreateAclsRequest original, final CreateAclsRequest actual) {
        List<CreatableAcl> originalCreations = original.data().creations();
        List<CreatableAcl> actualCreations = actual.data().creations();
        assertEquals("Number of Acls wrong", originalCreations.size(), actualCreations.size());

        for (int idx = 0; idx != originalCreations.size(); ++idx) {
            final CreatableAcl originalBinding = originalCreations.get(idx);
            final CreatableAcl actualBinding = actualCreations.get(idx);
            assertEquals(originalBinding, actualBinding);
        }
    }

}