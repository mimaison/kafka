package org.apache.kafka.clients.admin;

import org.apache.kafka.common.protocol.ApiKeys;

public class ApiVersion {
    public final short apiKey;
    public final short minVersion;
    public final short maxVersion;

    public ApiVersion(ApiKeys apiKey) {
        this(apiKey.id, apiKey.oldestVersion(), apiKey.latestVersion());
    }

    public ApiVersion(ApiKeys apiKey, short minVersion, short maxVersion) {
        this(apiKey.id, minVersion, maxVersion);
    }

    public ApiVersion(short apiKey, short minVersion, short maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    @Override
    public String toString() {
        return "ApiVersion(" +
                "apiKey=" + apiKey +
                ", minVersion=" + minVersion +
                ", maxVersion= " + maxVersion +
                ")";
    }
}
