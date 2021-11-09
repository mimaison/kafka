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
package org.apache.kafka.clients.admin;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TagPredicate {
    private final OpType op;
    private final String key;
    private final Set<String> values;
    private final Optional<String> prefix;

    public enum OpType {
        EQUAL((byte) 0),
        NOT_EQUAL((byte) 1),
        IN((byte) 2),
        HAS((byte) 3);

        private static final Map<Byte, TagPredicate.OpType> OP_TYPES = Collections.unmodifiableMap(
                Arrays.stream(values()).collect(Collectors.toMap(TagPredicate.OpType::id, Function.identity()))
        );

        private final byte id;

        OpType(final byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static TagPredicate.OpType forId(final byte id) {
            return OP_TYPES.get(id);
        }
    }

    TagPredicate(OpType op, Optional<String> prefix, String key, Set<String> values) {
        this.op = op;
        this.prefix = prefix;
        this.key = key;
        this.values = values;
    }

    public OpType opType() {
        return op;
    }

    public String key() {
        return key;
    }

    public Set<String> values() {
        return values;
    }

    public static TagPredicate equal(String key, String value) {
        return new TagPredicate(OpType.EQUAL, Optional.empty(), key, Collections.singleton(value));
    }

    public static TagPredicate notEqual(String key, String value) {
        return new TagPredicate(OpType.NOT_EQUAL, Optional.empty(), key, Collections.singleton(value));
    }

    public static TagPredicate in(String key, Set<String> values) {
        return new TagPredicate(OpType.IN, Optional.empty(), key, values);
    }

    @Override
    public String toString() {
        return "TagPredicate{" +
                "op=" + op +
                ", key='" + key + '\'' +
                ", values=" + values +
                ", prefix=" + prefix +
                '}';
    }

    public static TagPredicate has(String key) {
        return new TagPredicate(OpType.HAS, Optional.empty(), key, Collections.emptySet());
    }
}

