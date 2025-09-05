package org.apache.kafka.server.telemetry;

import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

/**
 * Context provided to {@link ClientTelemetryReceiver} implementations when receiving client metrics.
 */
public interface ClientTelemetryContext extends AuthorizableRequestContext {

    /**
     * The interval defined via <code>metrics.interval</code> in the client metrics subscription
     * @return The interval in milliseconds
     */
    int pushIntervalMs();

    /**
     * The context associated with this request
     * @return The AuthorizableRequestContext associated with this request
     */
    AuthorizableRequestContext authorizableRequestContext();
}
