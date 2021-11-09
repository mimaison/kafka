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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class representing an alter tag entry containing name, value and operation type.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class AlterTagOp {

    public enum OpType {
        /**
         * Set the value of the tag entry.
         */
        SET((byte) 0),
        /**
         * Delete the tag entry.
         */
        DELETE((byte) 1);

        private static final Map<Byte, OpType> OP_TYPES = Collections.unmodifiableMap(
                Arrays.stream(values()).collect(Collectors.toMap(OpType::id, Function.identity()))
        );

        private final byte id;

        OpType(final byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static OpType forId(final byte id) {
            return OP_TYPES.get(id);
        }
    }

    private final Properties tags;
    private final OpType opType;

    public AlterTagOp(Properties tags, OpType operationType) {
        this.tags = tags;
        this.opType =  operationType;
    }

    public Properties tags() {
        return tags;
    };

    public OpType opType() {
        return opType;
    };

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AlterTagOp that = (AlterTagOp) o;
        return opType == that.opType &&
                Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, tags);
    }

    @Override
    public String toString() {
        return "AlterTagOp{" +
                "opType=" + opType +
                ", tags=" + tags +
                '}';
    }
}
