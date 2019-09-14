package org.apache.kafka.clients.admin;

import java.util.Map;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;

public class DescribeApiVersionsResult {
    
    Map<Node, KafkaFutureImpl<NodeApiVersions>> futures;

    public DescribeApiVersionsResult(Map<Node, KafkaFutureImpl<NodeApiVersions>> futures) {
        this.futures = futures;
    }
    
    public Map<Node, KafkaFutureImpl<NodeApiVersions>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds only if all the config descriptions succeed.
     */
    public KafkaFuture<Map<Node, KafkaFutureImpl<NodeApiVersions>>> all() {
        return null;
    }

}
