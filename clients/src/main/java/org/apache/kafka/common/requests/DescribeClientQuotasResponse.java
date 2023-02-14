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

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeClientQuotasResponse extends AbstractResponse {

    private final DescribeClientQuotasResponseData data;

    public DescribeClientQuotasResponse(DescribeClientQuotasResponseData data) {
        super(ApiKeys.DESCRIBE_CLIENT_QUOTAS);
        this.data = data;
    }

    public void complete(KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>> future) {
        Errors error = Errors.forCode(data.errorCode());
        if (error != Errors.NONE) {
            future.completeExceptionally(error.exception(data.errorMessage()));
            return;
        }

        Map<ClientQuotaEntity, Map<String, Double>> result = new HashMap<>(data.entries().size());
        for (DescribeClientQuotasResponseData.EntryData entries : data.entries()) {
            Map<String, String> entity = new HashMap<>(entries.entity().size());
            for (DescribeClientQuotasResponseData.EntityData entityData : entries.entity()) {
                entity.put(entityData.entityType(), entityData.entityName());
            }

            Map<String, Double> values = new HashMap<>(entries.values().size());
            for (DescribeClientQuotasResponseData.ValueData valueData : entries.values()) {
                values.put(valueData.key(), valueData.value());
            }

            result.put(new ClientQuotaEntity(entity), values);
        }
        future.complete(result);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public DescribeClientQuotasResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    public static DescribeClientQuotasResponse parse(ByteBuffer buffer, short version) {
        return new DescribeClientQuotasResponse(new DescribeClientQuotasResponseData(new ByteBufferAccessor(buffer), version));
    }

    public static DescribeClientQuotasResponse fromQuotaEntities(Map<ClientQuotaEntity, Map<String, Double>> entities,
                                                                 int throttleTimeMs) {
        List<DescribeClientQuotasResponseData.EntryData> entries = new ArrayList<>(entities.size());
        for (Map.Entry<ClientQuotaEntity, Map<String, Double>> entry : entities.entrySet()) {
            ClientQuotaEntity quotaEntity = entry.getKey();
            List<DescribeClientQuotasResponseData.EntityData> entityData = new ArrayList<>(quotaEntity.entries().size());
            for (Map.Entry<String, String> entityEntry : quotaEntity.entries().entrySet()) {
                entityData.add(new DescribeClientQuotasResponseData.EntityData()
                        .setEntityType(entityEntry.getKey())
                        .setEntityName(entityEntry.getValue()));
            }

            Map<String, Double> quotaValues = entry.getValue();
            List<DescribeClientQuotasResponseData.ValueData> valueData = new ArrayList<>(quotaValues.size());
            for (Map.Entry<String, Double> valuesEntry : entry.getValue().entrySet()) {
                valueData.add(new DescribeClientQuotasResponseData.ValueData()
                        .setKey(valuesEntry.getKey())
                        .setValue(valuesEntry.getValue()));
            }

            entries.add(new DescribeClientQuotasResponseData.EntryData()
                    .setEntity(entityData)
                    .setValues(valueData));
        }

        return new DescribeClientQuotasResponse(new DescribeClientQuotasResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode((short) 0)
            .setErrorMessage(null)
            .setEntries(entries));
    }
}
