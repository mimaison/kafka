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
package org.apache.kafka.tools;

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolAssignmentJsonConverter;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.message.ConsumerProtocolSubscriptionJsonConverter;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.KRaftVersionRecordJsonConverter;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotFooterRecordJsonConverter;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecordJsonConverter;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.message.VotersRecordJsonConverter;
import org.apache.kafka.common.metadata.MetadataJsonConverters;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.AbstractLegacyRecordBatch;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecordSerde;
import org.apache.kafka.coordinator.common.runtime.Deserializer;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordSerde;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValueJsonConverter;
import org.apache.kafka.coordinator.share.ShareCoordinatorRecordSerde;
import org.apache.kafka.coordinator.transaction.TransactionCoordinatorRecordSerde;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.snapshot.SnapshotPath;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.CorruptSnapshotException;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.OffsetPosition;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TimestampOffset;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.tools.api.Decoder;
import org.apache.kafka.tools.api.StringDecoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

public class DumpLogSegments {

    private static final String RECORD_INDENT = "|";

    public static void main(String[] args) throws Exception {
        DumpLogSegmentsOptions opts = new DumpLogSegmentsOptions(args);
        CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to parse a log file and dump its contents to the console, useful for debugging a seemingly corrupt log segment.");
        opts.checkArgs();

        Map<String, List<Map.Entry<Long, Long>>> misMatchesForIndexFilesMap = new HashMap<>();
        TimeIndexDumpErrors timeIndexDumpErrors = new TimeIndexDumpErrors();
        Map<String, List<Map.Entry<Long, Long>>> nonConsecutivePairsForLogFilesMap = new HashMap<>();

        for (String arg : opts.files()) {
            File file = new File(arg);
            System.out.println("Dumping " + file);

            String filename = file.getName();
            String suffix = filename.substring(filename.lastIndexOf("."));
            switch (suffix) {
                case UnifiedLog.LOG_FILE_SUFFIX, Snapshots.SUFFIX ->
                     dumpLog(file, opts.shouldPrintDataLog(), nonConsecutivePairsForLogFilesMap, opts.isDeepIteration(),
                                     opts.messageParser(), opts.skipRecordMetadata(), opts.maxBytes());
                case UnifiedLog.INDEX_FILE_SUFFIX ->
                     dumpIndex(file, opts.indexSanityOnly(), opts.verifyOnly(), misMatchesForIndexFilesMap, opts.maxMessageSize());
                case UnifiedLog.TIME_INDEX_FILE_SUFFIX ->
                     dumpTimeIndex(file, opts.indexSanityOnly(), opts.verifyOnly(), timeIndexDumpErrors);
                case LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX ->
                     dumpProducerIdSnapshot(file);
                case UnifiedLog.TXN_INDEX_FILE_SUFFIX ->
                     dumpTxnIndex(file);
                default ->
                     System.err.println("Ignoring unknown file " + file);
            }
        }

        misMatchesForIndexFilesMap.forEach((filename, listOfMismatches) -> {
            System.err.println("Mismatches in :" + filename);
            listOfMismatches.forEach(entry ->
                System.err.println("  Index offset: " + entry.getKey() + ", log offset: " + entry.getValue())
            );
        });

        timeIndexDumpErrors.printErrors();

        nonConsecutivePairsForLogFilesMap.forEach((filename, listOfNonConsecutivePairs) -> {
            System.err.println("Non-consecutive offsets in " + filename);
            listOfNonConsecutivePairs.forEach(entry ->
                System.err.println("  " + entry.getKey() + " is followed by " + entry.getValue())
            );
        });
    }

    private static void dumpTxnIndex(File file) throws IOException {
        try (TransactionIndex index = new TransactionIndex(UnifiedLog.offsetFromFile(file), file)) {
            for (AbortedTxn abortedTxn : index.allAbortedTxns()) {
                System.out.printf("version: %d producerId: %d firstOffset: %d lastOffset: %d lastStableOffset: %d%n",
                        abortedTxn.version(), abortedTxn.producerId(), abortedTxn.firstOffset(), abortedTxn.lastOffset(), abortedTxn.lastStableOffset());
            }
        }
    }

    private static void dumpProducerIdSnapshot(File file) throws IOException {
        try {
            ProducerStateManager.readSnapshot(file).forEach(entry -> {
                System.out.printf("producerId: %d producerEpoch: %d" +
                        " coordinatorEpoch: %d currentTxnFirstOffset: %s" +
                        " lastTimestamp: %d ", entry.producerId(), entry.producerEpoch(), entry.producerEpoch(), entry.currentTxnFirstOffset(), entry.lastTimestamp());
                if (!entry.batchMetadata().isEmpty()) {
                    BatchMetadata metadata = entry.batchMetadata().iterator().next();
                    System.out.printf("firstSequence: %d lastSequence: %d lastOffset: %d offsetDelta: %d timestamp: %d",
                            metadata.firstSeq(), metadata.lastSeq, metadata.lastOffset, metadata.offsetDelta, metadata.timestamp);
                }
                System.out.println();
            });
        } catch (CorruptSnapshotException e) {
            System.err.println(e.getMessage());
        }
    }

    /* print out the contents of the index */
    // Visible for testing
    private static void dumpIndex(File file,
                           boolean indexSanityOnly,
                           boolean verifyOnly,
                           Map<String, List<Map.Entry<Long, Long>>> misMatchesForIndexFilesMap,
                           int maxMessageSize) throws IOException {
        long startOffset = Long.parseLong(file.getName().split("\\.")[0]);
        File logFile = new File(file.getAbsoluteFile().getParent(), file.getName().split("\\.")[0] + UnifiedLog.LOG_FILE_SUFFIX);
        FileRecords fileRecords = FileRecords.open(logFile, false);
        OffsetIndex index = new OffsetIndex(file, startOffset, -1, false);

        if (index.entries() == 0) {
            System.out.println(file + " is empty.");
            return;
        }

        //Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
        if (indexSanityOnly) {
            index.sanityCheck();
            System.out.println(file + " passed sanity check.");
            return;
        }

        for (int i = 0; i < index.entries(); i++) {
            OffsetPosition entry = index.entry(i);

            // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
            if (entry.offset == index.baseOffset() && i > 0)
                return;

            FileRecords slice = fileRecords.slice(entry.position, maxMessageSize);
            long firstBatchLastOffset = slice.batches().iterator().next().lastOffset();
            if (firstBatchLastOffset != entry.offset) {
                List<Map.Entry<Long, Long>> mismatches = misMatchesForIndexFilesMap.computeIfAbsent(file.getAbsolutePath(), k -> new ArrayList<>());
                mismatches.add(Map.entry(entry.offset, firstBatchLastOffset));
            }
            if (!verifyOnly)
                System.out.println("offset: " + entry.offset + " position: " + entry.position);
        }
    }

    // Visible for testing
    private static void dumpTimeIndex(File file,
                               boolean indexSanityOnly,
                               boolean verifyOnly,
                               TimeIndexDumpErrors timeIndexDumpErrors) throws IOException {
        long startOffset = Long.parseLong(file.getName().split("\\.")[0]);
        File logFile = new File(file.getAbsoluteFile().getParent(), file.getName().split("\\.")[0] + UnifiedLog.LOG_FILE_SUFFIX);
        FileRecords fileRecords = FileRecords.open(logFile, false);
        File indexFile = new File(file.getAbsoluteFile().getParent(), file.getName().split("\\.")[0] + UnifiedLog.INDEX_FILE_SUFFIX);
        OffsetIndex index = new OffsetIndex(indexFile, startOffset, -1, false);
        TimeIndex timeIndex = new TimeIndex(file, startOffset, -1, false);

        try {
            //Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
            if (indexSanityOnly) {
                timeIndex.sanityCheck();
                System.out.println(file + " passed sanity check.");
                return;
            }

            var prevTimestamp = RecordBatch.NO_TIMESTAMP;
            for (int i = 0; i < timeIndex.entries(); i++) {
                TimestampOffset entry = timeIndex.entry(i);

                // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
                if (entry.offset == timeIndex.baseOffset() && i > 0)
                    return;

                int position = index.lookup(entry.offset).position;
                FileRecords partialFileRecords = fileRecords.slice(position, Integer.MAX_VALUE);
                Iterable<FileLogInputStream.FileChannelRecordBatch> batches = partialFileRecords.batches();
                var maxTimestamp = RecordBatch.NO_TIMESTAMP;
                // We first find the message by offset then check if the timestamp is correct.
                FileLogInputStream.FileChannelRecordBatch batch = null;
                for (FileLogInputStream.FileChannelRecordBatch b : batches) {
                    if (b.lastOffset() >= entry.offset) {
                        batch = b;
                        break;
                    }
                }
                if (batch == null) {
                    timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset, -1L);
                } else if (batch.lastOffset() != entry.offset) {
                    timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset, batch.lastOffset());
                } else {
                    for (Record record : batch) {
                        maxTimestamp = Math.max(maxTimestamp, record.timestamp());
                    }
                    if (maxTimestamp != entry.timestamp)
                        timeIndexDumpErrors.recordMismatchTimeIndex(file, entry.timestamp, maxTimestamp);

                    if (prevTimestamp >= entry.timestamp)
                        timeIndexDumpErrors.recordOutOfOrderIndexTimestamp(file, entry.timestamp, prevTimestamp);
                }
                if (!verifyOnly)
                    System.out.println("timestamp: " + entry.timestamp + " offset: " + entry.offset);
                prevTimestamp = entry.timestamp;
            }
        } finally {
            fileRecords.closeHandlers();
            index.closeHandler();
            timeIndex.closeHandler();
        }
    }

    interface MessageParser<K, V> {
        Map.Entry<Optional<K>, Optional<V>> parse(Record record);
    }

    private record DecoderMessageParser<K, V>(Decoder<K> keyDecoder, Decoder<V> valueDecoder) implements MessageParser<K, V> {

        @Override
        public Map.Entry<Optional<K>, Optional<V>> parse(Record record) {
            Optional<K> key = record.hasKey()
                    ? Optional.of(keyDecoder.fromBytes(Utils.readBytes(record.key())))
                    : Optional.empty();
            if (!record.hasValue()) {
                return Map.entry(key, Optional.empty());
            } else {
                Optional<V> value = Optional.of(valueDecoder.fromBytes(Utils.readBytes(record.value())));
                return Map.entry(key, value);
            }
        }
    }

    /* print out the contents of the log */
    private static void dumpLog(File file,
                         boolean printContents,
                         Map<String, List<Map.Entry<Long, Long>>> nonConsecutivePairsForLogFilesMap,
                         boolean isDeepIteration,
                         MessageParser<?, ?> parser,
                         boolean skipRecordMetadata,
                         int maxBytes) throws IOException {
        if (file.getName().endsWith(UnifiedLog.LOG_FILE_SUFFIX)) {
            long startOffset = Long.parseLong(file.getName().split("\\.")[0]);
            System.out.println("Log starting offset: " + startOffset);
        } else if (file.getName().endsWith(Snapshots.SUFFIX)) {
            if (BootstrapDirectory.BINARY_BOOTSTRAP_FILENAME.equals(file.getName())) {
                System.out.println("KRaft bootstrap snapshot");
            } else {
                SnapshotPath path = Snapshots.parse(file.toPath()).get();
                System.out.println("Snapshot end offset: " + path.snapshotId().offset() + ", epoch: " + path.snapshotId().epoch());
            }
        }
        FileRecords fileRecords = FileRecords.open(file, false).slice(0, maxBytes);
        try {
            long validBytes = 0L;
            long lastOffset = -1L;

            for (FileLogInputStream.FileChannelRecordBatch batch : fileRecords.batches()) {
                printBatchLevel(batch, validBytes);
                if (isDeepIteration) {
                    for (Record record : batch) {
                        if (record.offset() != lastOffset + 1) {
                            List<Map.Entry<Long, Long>> nonConsecutivePairs = nonConsecutivePairsForLogFilesMap.computeIfAbsent(file.getAbsolutePath(), k -> new ArrayList<>());
                            nonConsecutivePairs.add(Map.entry(lastOffset, record.offset()));
                        }
                        lastOffset = record.offset();

                        String prefix = RECORD_INDENT + " ";
                        if (!skipRecordMetadata) {
                            System.out.printf("%soffset: %d %s: %d keySize: %d valueSize: %d",
                                    prefix, record.offset(), batch.timestampType(), record.timestamp(), record.keySize(), record.valueSize());
                            prefix = " ";

                            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                                System.out.print(" sequence: " + record.sequence() + " headerKeys: " + Arrays.stream(record.headers()).map(Header::key).collect(Collectors.joining(", ", "[", "]")));
                            }
                            if (record instanceof AbstractLegacyRecordBatch r) {
                                System.out.printf(" isValid: %s crc: %d", r.isValid(), r.checksum());
                            }

                            if (batch.isControlBatch()) {
                                short controlTypeId = ControlRecordType.parseTypeId(record.key());
                                switch (ControlRecordType.fromTypeId(controlTypeId)) {
                                    case ABORT, COMMIT -> {
                                        EndTransactionMarker endTxnMarker = EndTransactionMarker.deserialize(record);
                                        System.out.print(" endTxnMarker: " + endTxnMarker.controlType() + " coordinatorEpoch: " + endTxnMarker.coordinatorEpoch());
                                    }
                                    case SNAPSHOT_HEADER -> {
                                        SnapshotHeaderRecord header = ControlRecordUtils.deserializeSnapshotHeaderRecord(record);
                                        System.out.print(" SnapshotHeader " + SnapshotHeaderRecordJsonConverter.write(header, header.version()));
                                    }
                                    case SNAPSHOT_FOOTER -> {
                                        SnapshotFooterRecord footer = ControlRecordUtils.deserializeSnapshotFooterRecord(record);
                                        System.out.print(" SnapshotFooter " + SnapshotFooterRecordJsonConverter.write(footer, footer.version()));
                                    }
                                    case KRAFT_VERSION -> {
                                        KRaftVersionRecord kraftVersion = ControlRecordUtils.deserializeKRaftVersionRecord(record);
                                        System.out.print(" KRaftVersion " + KRaftVersionRecordJsonConverter.write(kraftVersion, kraftVersion.version()));
                                    }
                                    case KRAFT_VOTERS -> {
                                        VotersRecord voters = ControlRecordUtils.deserializeVotersRecord(record);
                                        System.out.print(" KRaftVoters " + VotersRecordJsonConverter.write(voters, voters.version()));
                                    }
                                    default ->
                                        System.out.println(" controlType: " + ControlRecordType.fromTypeId(controlTypeId) + "(" +  controlTypeId + ")");
                                }
                            }
                        }
                        if (printContents && !batch.isControlBatch()) {
                            Map.Entry<? extends Optional<?>, ? extends Optional<?>> entry = parser.parse(record);
                            if (entry.getKey().isPresent()) {
                                System.out.printf("%skey: %s%n", prefix, entry.getKey());
                            }
                            entry.getValue().ifPresent(value ->
                                System.out.printf(" payload: %s", value)
                            );
                        }
                        System.out.println();
                    }
                }
                validBytes += batch.sizeInBytes();
            }
            long trailingBytes = fileRecords.sizeInBytes() - validBytes;
            if ((trailingBytes > 0) && (maxBytes == Integer.MAX_VALUE))
                System.out.println("Found " + trailingBytes + " invalid bytes at the end of " + file.getName());
        } finally {
            fileRecords.closeHandlers();
        }
    }

    private static void printBatchLevel(FileLogInputStream.FileChannelRecordBatch batch, long accumulativeBytes) {
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2)
            System.out.println("baseOffset: " + batch.baseOffset() + " lastOffset: " + batch.lastOffset() + " count: " + batch.countOrNull() +
                    " baseSequence: " + batch.baseSequence() + " lastSequence: " + batch.lastSequence() +
                    " producerId: " + batch.producerId() + " producerEpoch: " + batch.producerEpoch() +
                    " partitionLeaderEpoch: " + batch.partitionLeaderEpoch() + " isTransactional: " + batch.isTransactional() +
                    " isControl: " + batch.isControlBatch() + " deleteHorizonMs: " + batch.deleteHorizonMs());
        else
            System.out.printf("offset: %s", batch.lastOffset());

        System.out.println(" position: " + accumulativeBytes + " " + batch.timestampType() + ": " + batch.maxTimestamp() +
                " size: " + batch.sizeInBytes() + " magic: " + batch.magic() +
                " compresscodec: " + batch.compressionType().name + " crc: " + batch.checksum() + " isvalid: " + batch.isValid());
    }

    static class TimeIndexDumpErrors {
        Map<String, List<Map.Entry<Long, Long>>> misMatchesForTimeIndexFilesMap = new HashMap<>();
        Map<String, List<Map.Entry<Long, Long>>> outOfOrderTimestamp = new HashMap<>();
        Map<String, List<Map.Entry<Long, Long>>> shallowOffsetNotFound = new HashMap<>();

        void recordMismatchTimeIndex(File file, long indexTimestamp, long logTimestamp) {
            List<Map.Entry<Long, Long>> misMatchesSeq = misMatchesForTimeIndexFilesMap.computeIfAbsent(file.getAbsolutePath(), k -> new ArrayList<>());
            misMatchesSeq.add(Map.entry(indexTimestamp, logTimestamp));
        }

        void recordOutOfOrderIndexTimestamp(File file, long indexTimestamp, long prevIndexTimestamp) {
            List<Map.Entry<Long, Long>> outOfOrderSeq = outOfOrderTimestamp.computeIfAbsent(file.getAbsolutePath(), k -> new ArrayList<>());
            outOfOrderSeq.add(Map.entry(indexTimestamp, prevIndexTimestamp));
        }

        void recordShallowOffsetNotFound(File file, long indexOffset, long logOffset) {
            List<Map.Entry<Long, Long>> shallowOffsetNotFoundSeq = shallowOffsetNotFound.computeIfAbsent(file.getAbsolutePath(), k -> new ArrayList<>());
            shallowOffsetNotFoundSeq.add(Map.entry(indexOffset, logOffset));
        }

        void printErrors() {
            misMatchesForTimeIndexFilesMap.forEach((filename, listOfMismatches) -> {
                System.err.println("Found timestamp mismatch in :" + filename);
                listOfMismatches.forEach(m ->
                        System.err.printf("  Index timestamp: %d, log timestamp: %d%n", m.getKey(), m.getValue())
                );
            });

            outOfOrderTimestamp.forEach((filename, outOfOrderTimestamps) -> {
                System.err.println("Found out of order timestamp in :" + filename);
                outOfOrderTimestamps.forEach(entry ->
                        System.err.printf("  Index timestamp: %d, Previously indexed timestamp: %d%n", entry.getKey(), entry.getValue())
                );
            });

            shallowOffsetNotFound.values().forEach(listOfShallowOffsetNotFound -> {
                System.err.println("The following indexed offsets are not found in the log.");
                listOfShallowOffsetNotFound.forEach(entry ->
                        System.err.printf("Indexed offset: %d, found log offset: %d%n", entry.getKey(), entry.getValue())
                );
            });
        }
    }

    abstract static class CoordinatorRecordMessageParser implements MessageParser<String, String> {

        private final CoordinatorRecordSerde serde;

        CoordinatorRecordMessageParser(CoordinatorRecordSerde serde) {
            this.serde = serde;
        }

        @Override
        public Map.Entry<Optional<String>, Optional<String>> parse(Record record) {
            if (!record.hasKey())
                throw new RuntimeException("Failed to decode message at offset " + record.offset() + " using the " +
                        "specified decoder (message had a missing key)");

            try {
                CoordinatorRecord r = serde.deserialize(record.key(), record.value());
                Optional<String> value = r.value() != null
                        ? Optional.of(prepareValue(r.value().message(), r.value().version()))
                        : Optional.of("<DELETE>");
                return Map.entry(
                        Optional.of(prepareKey(r.key())),
                        value
                );
            } catch (Deserializer.UnknownRecordTypeException urte) {
                return Map.entry(
                        Optional.of("Unknown record type " + urte.unknownType() + " at offset " + record.offset() + ", skipping."),
                        Optional.empty());
            } catch (Throwable e) {
                return Map.entry(
                        Optional.of("Error at offset " + record.offset() + ", skipping. " + e.getMessage()),
                        Optional.empty());
            }
        }

        private String prepareKey(ApiMessage message) {
            ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
            json.set("type", new TextNode(String.valueOf(message.apiKey())));
            json.set("data", keyAsJson(message));
            return json.toString();
        }

        private String prepareValue(ApiMessage message, short version) {
            ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
            json.set("version", new TextNode(String.valueOf(version)));
            json.set("data", valueAsJson(message, version));
            return json.toString();
        }

        protected abstract JsonNode keyAsJson(ApiMessage message);
        protected abstract JsonNode valueAsJson(ApiMessage message, short version);
    }

    // Package private for testing.
    static class OffsetsMessageParser extends CoordinatorRecordMessageParser {
        OffsetsMessageParser() {
            super(new GroupCoordinatorRecordSerde());
        }

        protected JsonNode keyAsJson(ApiMessage message) {
            return org.apache.kafka.coordinator.group.generated.CoordinatorRecordJsonConverters.writeRecordKeyAsJson(message);
        }

        protected JsonNode valueAsJson(ApiMessage message, short version) {
            if (message.apiKey() == org.apache.kafka.coordinator.group.generated.CoordinatorRecordType.GROUP_METADATA.id()) {
                return prepareGroupMetadataValue((org.apache.kafka.coordinator.group.generated.GroupMetadataValue) message, version);
            } else {
                return org.apache.kafka.coordinator.group.generated.CoordinatorRecordJsonConverters.writeRecordValueAsJson(message, version);
            }
        }

        <T> void replace(
                JsonNode node,
                String field,
                BiFunction<Readable, Short, T> reader,
                BiFunction<T, Short, JsonNode> writer) {
            Optional.ofNullable(node.get(field)).ifPresent(filedNode -> {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(filedNode.binaryValue());
                    ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
                    short version = accessor.readShort();
                    T data = reader.apply(accessor, version);
                    ((ObjectNode) node).replace(field, writer.apply(data, version));
                } catch (RuntimeException | IOException e) {
                    // Swallow and keep the original bytes.
                }
            });
        }

        private JsonNode prepareGroupMetadataValue(GroupMetadataValue message, short version) {
            JsonNode json = GroupMetadataValueJsonConverter.write(message, version);

            Optional.ofNullable(json.get("protocolType")).ifPresent(protocolTypeNode -> {
                // If the group uses the consumer embedded protocol, we deserialize
                // the subscription and the assignment of each member.
                if (ConsumerProtocol.PROTOCOL_TYPE.equals(protocolTypeNode.asText())) {
                    Optional.ofNullable(json.get("members")).ifPresent(membersNode -> {
                        if (membersNode.isArray()) {
                            membersNode.forEach(memberNode -> {
                                // Replace the subscription field by its deserialized version.
                                replace(memberNode,
                                        "subscription",
                                        ConsumerProtocolSubscription::new,
                                        ConsumerProtocolSubscriptionJsonConverter::write);

                                // Replace the assignment field by its deserialized version.
                                replace(memberNode,
                                        "assignment",
                                        ConsumerProtocolAssignment::new,
                                        ConsumerProtocolAssignmentJsonConverter::write);
                            });
                        }
                    });
                }
            });
            return json;
        }
    }

    // Package private for testing.
    static class TransactionLogMessageParser extends CoordinatorRecordMessageParser {

        TransactionLogMessageParser() {
            super(new TransactionCoordinatorRecordSerde());
        }

        protected JsonNode keyAsJson(ApiMessage message) {
            return org.apache.kafka.coordinator.transaction.generated.CoordinatorRecordJsonConverters.writeRecordKeyAsJson(message);
        }

        protected JsonNode valueAsJson(ApiMessage message, short version) {
            return org.apache.kafka.coordinator.transaction.generated.CoordinatorRecordJsonConverters.writeRecordValueAsJson(message, version);
        }
    }

    private static class ClusterMetadataLogMessageParser implements MessageParser<String, String> {
        private final MetadataRecordSerde metadataRecordSerde = new MetadataRecordSerde();

        @Override
        public Map.Entry<Optional<String>, Optional<String>> parse(Record record) {
            String output;
            try {
                ApiMessageAndVersion messageAndVersion = metadataRecordSerde.read(new ByteBufferAccessor(record.value()), record.valueSize());
                ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
                json.set("type", new TextNode(String.valueOf(MetadataRecordType.fromId(messageAndVersion.message().apiKey()))));
                json.set("version", new IntNode(messageAndVersion.version()));
                json.set("data", MetadataJsonConverters.writeJson(messageAndVersion.message(), messageAndVersion.version()));
                output = json.toString();
            } catch (Throwable e) {
                output = "Error at " + record.offset() + ", skipping. " + e.getMessage();
            }
            // No keys for metadata records
            return Map.entry(Optional.empty(), Optional.of(output));
        }
    }

    private static class RemoteMetadataLogMessageParser implements MessageParser<String, String> {
        private final RemoteLogMetadataSerde metadataRecordSerde = new RemoteLogMetadataSerde();

        @Override
        public Map.Entry<Optional<String>, Optional<String>> parse(Record record) {
            String output;
            try {
                ByteBuffer data = ByteBuffer.allocate(record.value().remaining());
                record.value().get(data.array());
                output = metadataRecordSerde.deserialize(data.array()).toString();
            } catch (Throwable e) {
                output = "Error at offset " + record.offset() + ", skipping. " + e.getMessage();
            }
            // No keys for metadata records
            return Map.entry(Optional.empty(), Optional.of(output));
        }
    }

    // for test visibility
    static class ShareGroupStateMessageParser extends CoordinatorRecordMessageParser {
        ShareGroupStateMessageParser() {
            super(new ShareCoordinatorRecordSerde());
        }

        @Override
        protected JsonNode keyAsJson(ApiMessage message) {
            return org.apache.kafka.coordinator.share.generated.CoordinatorRecordJsonConverters.writeRecordKeyAsJson(message);
        }

        @Override protected JsonNode valueAsJson(ApiMessage message, short version) {
            return org.apache.kafka.coordinator.share.generated.CoordinatorRecordJsonConverters.writeRecordValueAsJson(message, version);
        }
    }

    private static class DumpLogSegmentsOptions extends CommandDefaultOptions {

        private final OptionSpecBuilder printOpt;
        private final OptionSpecBuilder verifyOpt;
        private final OptionSpecBuilder indexSanityOpt;
        private final OptionSpec<String> filesOpt;
        private final OptionSpec<Integer> maxMessageSizeOpt;
        private final OptionSpec<Integer> maxBytesOpt;
        private final OptionSpecBuilder deepIterationOpt;
        private final OptionSpec<String> valueDecoderOpt;
        private final OptionSpec<String> keyDecoderOpt;
        private final OptionSpecBuilder offsetsOpt;
        private final OptionSpecBuilder transactionLogOpt;
        private final OptionSpecBuilder clusterMetadataOpt;
        private final OptionSpecBuilder remoteMetadataOpt;
        private final OptionSpecBuilder shareStateOpt;
        private final OptionSpecBuilder skipRecordMetadataOpt;

        DumpLogSegmentsOptions(String[] args) {
            super(args);
            printOpt = parser.accepts("print-data-log", "If set, printing the messages content when dumping data logs. Automatically set if any decoder option is specified.");
            verifyOpt = parser.accepts("verify-index-only", "If set, just verify the index log without printing its content.");
            indexSanityOpt = parser.accepts("index-sanity-check", "If set, just checks the index sanity without printing its content. " +
                    "This is the same check that is executed on broker startup to determine if an index needs rebuilding or not.");
            filesOpt = parser.accepts("files", "REQUIRED: The comma separated list of data and index log files to be dumped.")
                    .withRequiredArg()
                    .describedAs("file1, file2, ...")
                    .ofType(String.class);
            maxMessageSizeOpt = parser.accepts("max-message-size", "Size of largest message.")
                    .withRequiredArg()
                    .describedAs("size")
                    .ofType(Integer.class)
                    .defaultsTo(5 * 1024 * 1024);
            maxBytesOpt = parser.accepts("max-bytes", "Limit the amount of total batches read in bytes avoiding reading the whole .log file(s).")
                    .withRequiredArg()
                    .describedAs("size")
                    .ofType(Integer.class)
                    .defaultsTo(Integer.MAX_VALUE);
            deepIterationOpt = parser.accepts("deep-iteration", "If set, uses deep instead of shallow iteration. Automatically set if print-data-log is enabled.");
            valueDecoderOpt = parser.accepts("value-decoder-class", "If set, used to deserialize the messages. This class should implement org.apache.kafka.tools.api.Decoder trait. Custom jar should be available in kafka/libs directory.")
                    .withOptionalArg()
                    .ofType(String.class)
                    .defaultsTo(StringDecoder.class.getName());
            keyDecoderOpt = parser.accepts("key-decoder-class", "If set, used to deserialize the keys. This class should implement org.apache.kafka.tools.api.Decoder trait. Custom jar should be available in kafka/libs directory.")
                    .withOptionalArg()
                    .ofType(String.class)
                    .defaultsTo(StringDecoder.class.getName());
            offsetsOpt = parser.accepts("offsets-decoder", "If set, log data will be parsed as offset data from the " +
                    "__consumer_offsets topic.");
            transactionLogOpt = parser.accepts("transaction-log-decoder", "If set, log data will be parsed as " +
                    "transaction metadata from the __transaction_state topic.");
            clusterMetadataOpt = parser.accepts("cluster-metadata-decoder", "If set, log data will be parsed as cluster metadata records.");
            remoteMetadataOpt = parser.accepts("remote-log-metadata-decoder", "If set, log data will be parsed as TopicBasedRemoteLogMetadataManager (RLMM) metadata records." +
                    " Instead, the value-decoder-class option can be used if a custom RLMM implementation is configured.");
            shareStateOpt = parser.accepts("share-group-state-decoder", "If set, log data will be parsed as share group state data from the " +
                    "__share_group_state topic.");
            skipRecordMetadataOpt = parser.accepts("skip-record-metadata", "Whether to skip printing metadata for each record.");
            options = parser.parse(args);
        }

        MessageParser<?, ?> messageParser() throws ClassNotFoundException {
            if (options.has(offsetsOpt)) {
                return new OffsetsMessageParser();
            }
            if (options.has(transactionLogOpt)) {
                return new TransactionLogMessageParser();
            }
            if (options.has(clusterMetadataOpt)) {
                return new ClusterMetadataLogMessageParser();
            }
            if (options.has(remoteMetadataOpt)) {
                return new RemoteMetadataLogMessageParser();
            }
            if (options.has(shareStateOpt)) {
                return new ShareGroupStateMessageParser();
            }
            Decoder<?> valueDecoder = Utils.newInstance(options.valueOf(valueDecoderOpt), Decoder.class);
            Decoder<?> keyDecoder = Utils.newInstance(options.valueOf(keyDecoderOpt), Decoder.class);
            return new DecoderMessageParser<>(keyDecoder, valueDecoder);
        }

        boolean shouldPrintDataLog() {
            return options.has(printOpt) ||
                    options.has(offsetsOpt) ||
                    options.has(transactionLogOpt) ||
                    options.has(clusterMetadataOpt) ||
                    options.has(remoteMetadataOpt) ||
                    options.has(valueDecoderOpt) ||
                    options.has(keyDecoderOpt) ||
                    options.has(shareStateOpt);
        }

        boolean skipRecordMetadata() {
            return options.has(skipRecordMetadataOpt);
        }
        boolean isDeepIteration() {
            return options.has(deepIterationOpt) || shouldPrintDataLog();
        }
        boolean verifyOnly() {
            return options.has(verifyOpt);
        }
        boolean indexSanityOnly() {
            return options.has(indexSanityOpt);
        }
        String[] files() {
            return options.valueOf(filesOpt).split(",");
        }
        int maxMessageSize() {
            return options.valueOf(maxMessageSizeOpt);
        }
        int maxBytes() {
            return options.valueOf(maxBytesOpt);
        }
        void checkArgs() {
            CommandLineUtils.checkRequiredArgs(parser, options, filesOpt);
        }
    }
}
