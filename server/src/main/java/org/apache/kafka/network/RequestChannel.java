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
package org.apache.kafka.network;

import org.apache.kafka.common.metrics.internals.MetricsUtils;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.metrics.RequestChannelMetrics;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class RequestChannel {

    private static final Logger LOG = LoggerFactory.getLogger(RequestChannel.class);

    private static final String REQUEST_QUEUE_SIZE_METRIC = "RequestQueueSize";
    private static final String RESPONSE_QUEUE_SIZE_METRIC = "ResponseQueueSize";
    public static final String PROCESSOR_METRIC_TAG = "processor";

    private final Time time;
    private final RequestChannelMetrics metrics;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.network", "RequestChannel");

    private final ArrayBlockingQueue<BaseRequest> requestQueue;
    private final ConcurrentHashMap<Integer, Processor> processors = new ConcurrentHashMap<>();
    private final ArrayBlockingQueue<BaseRequest> callbackQueue;

    public RequestChannel(int queueSize, Time time, RequestChannelMetrics metrics) {
        this.time = time;
        this.metrics = metrics;
        this.requestQueue = new ArrayBlockingQueue<>(queueSize);
        this.callbackQueue = new ArrayBlockingQueue<>(queueSize);

        metricsGroup.newGauge(REQUEST_QUEUE_SIZE_METRIC, requestQueue::size);
        metricsGroup.newGauge(RESPONSE_QUEUE_SIZE_METRIC, () ->
            processors.values().stream().mapToInt(Processor::responseQueueSize).sum()
        );
    }

    public RequestChannelMetrics metrics() {
        return metrics;
    }

    public void addProcessor(Processor processor) {
        if (processors.putIfAbsent(processor.id(), processor) != null)
            LOG.warn("Unexpected processor with processorId {}", processor.id());

        metricsGroup.newGauge(RESPONSE_QUEUE_SIZE_METRIC, processor::responseQueueSize,
            MetricsUtils.getTags(PROCESSOR_METRIC_TAG, String.valueOf(processor.id())));
    }

    public void removeProcessor(int processorId) {
        processors.remove(processorId);
        metricsGroup.removeMetric(RESPONSE_QUEUE_SIZE_METRIC,
            MetricsUtils.getTags(PROCESSOR_METRIC_TAG, String.valueOf(processorId)));
    }

    /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
    public void sendRequest(Request request) throws InterruptedException {
        requestQueue.put(request);
    }

    public void closeConnection(Request request, Map<Errors, Integer> errorCounts) {
        updateErrorMetrics(request.header().apiKey(), errorCounts);
        sendResponse(new CloseConnectionResponse(request));
    }

    public void sendResponse(Request request, AbstractResponse response) {
        updateErrorMetrics(request.header().apiKey(), response.errorCounts());
        sendResponse(new SendResponse(
            request,
            request.buildResponseSend(response),
            request.responseNode(response)
        ));
    }

    public void sendNoOpResponse(Request request) {
        sendResponse(new NoOpResponse(request));
    }

    public void startThrottling(Request request) {
        sendResponse(new StartThrottlingResponse(request));
    }

    public void endThrottling(Request request) {
        sendResponse(new EndThrottlingResponse(request));
    }

    /** Send a response back to the socket server to be sent over the network */
    public void sendResponse(Response response) {
        if (LOG.isTraceEnabled()) {
            var requestHeader = response.request().headerForLoggingOrThrottling();
            String message;
            if (response instanceof SendResponse sendResponse) {
                message = "Sending " + requestHeader.apiKey() + " response to client " + requestHeader.clientId() + " of " + sendResponse.responseSend().size() + " bytes.";
            } else if (response instanceof NoOpResponse) {
                message = "Not sending " + requestHeader.apiKey() + " response to client " + requestHeader.clientId() + " as it's not required.";
            } else if (response instanceof CloseConnectionResponse) {
                message = "Closing connection for client " + requestHeader.clientId() + " due to error during " + requestHeader.apiKey() + ".";
            } else if (response instanceof StartThrottlingResponse) {
                message = "Notifying channel throttling has started for client " + requestHeader.clientId() + " for " + requestHeader.apiKey();
            } else if (response instanceof EndThrottlingResponse) {
                message = "Notifying channel throttling has ended for client " + requestHeader.clientId() + " for " + requestHeader.apiKey();
            } else {
                message = "Unknown response type: " + response.getClass().getSimpleName();
            }
            LOG.trace(message);
        }

        if (response instanceof SendResponse || response instanceof NoOpResponse || response instanceof CloseConnectionResponse) {
            Request request = response.request();
            long timeNanos = time.nanoseconds();
            request.responseCompleteTimeNanos(timeNanos);
            if (request.apiLocalCompleteTimeNanos() == -1L)
                request.apiLocalCompleteTimeNanos(timeNanos);
            if (request.callbackRequestDequeueTimeNanos().isPresent() && request.callbackRequestCompleteTimeNanos().isEmpty())
                request.callbackRequestCompleteTimeNanos(OptionalLong.of(time.nanoseconds()));
        }

        Processor processor = processors.get(response.request().processor());
        if (processor != null) {
            processor.enqueueResponse(response);
        }
    }

    /**
     * Get the next request or block until specified time has elapsed.
     * Check the callback queue and execute first if present since these
     * requests have already waited in line.
     */
    public BaseRequest receiveRequest(long timeout) throws InterruptedException {
        BaseRequest callbackRequest = callbackQueue.poll();
        if (callbackRequest != null)
            return callbackRequest;

        BaseRequest request = requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
        if (request instanceof WakeupRequest) {
            return callbackQueue.poll();
        }
        return request;
    }

    public void updateErrorMetrics(ApiKeys apiKey, Map<Errors, Integer> errors) {
        errors.forEach((error, count) ->
            metrics.get(apiKey.name).markErrorMeter(error, count)
        );
    }

    public void clear() {
        requestQueue.clear();
        callbackQueue.clear();
    }

    public void shutdown() {
        clear();
        metrics.close();
    }

    public void sendShutdownRequest() throws InterruptedException {
        requestQueue.put(ShutdownRequest.INSTANCE);
    }

    public void sendCallbackRequest(CallbackRequest request) throws InterruptedException {
        callbackQueue.put(request);
        if (!requestQueue.offer(WakeupRequest.INSTANCE))
            LOG.trace("Wakeup request could not be added to queue. This means queue is full, so we will still process callback.");
    }
}