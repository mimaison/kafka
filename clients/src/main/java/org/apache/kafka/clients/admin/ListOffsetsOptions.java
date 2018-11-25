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
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;

/**
 * Options for {@link AdminClient#listOffsets()}.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListOffsetsOptions extends AbstractOptions<ListOffsetsOptions> {

    private final long offset;
    private final IsolationLevel isolationLevel;

    public static ListOffsetsOptions latestUncommitted() {
        return new ListOffsetsOptions(ListOffsetRequest.LATEST_TIMESTAMP, IsolationLevel.READ_UNCOMMITTED);
    }

    public static ListOffsetsOptions latestCommitted() {
        return new ListOffsetsOptions(ListOffsetRequest.LATEST_TIMESTAMP, IsolationLevel.READ_COMMITTED);
    }

    public static ListOffsetsOptions earliestUncommitted() {
        return new ListOffsetsOptions(ListOffsetRequest.EARLIEST_TIMESTAMP, IsolationLevel.READ_UNCOMMITTED);
    }

    public static ListOffsetsOptions earliestCommitted() {
        return new ListOffsetsOptions(ListOffsetRequest.EARLIEST_TIMESTAMP, IsolationLevel.READ_COMMITTED);
    }

    public ListOffsetsOptions(long offset, IsolationLevel isolationLevel) {
        this.offset = offset;
        this.isolationLevel = isolationLevel;
    }

    public long offset() {
        return offset;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }
}
