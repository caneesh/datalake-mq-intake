package com.hcsc.datalake.mqintake.core.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Root configuration properties for the MQ intake service.
 */
@ConfigurationProperties(prefix = "intake")
public class IntakeProperties {

    /**
     * List of binding configurations.
     */
    private List<BindingConfig> bindings = new ArrayList<>();

    /**
     * MAXUMSGS limit from the queue manager.
     * Default is 10,000 (common IBM MQ default).
     */
    private int maxumsgs = 10_000;

    /**
     * Aggregate memory ceiling in bytes across all bindings.
     * Sum of (batchBytes * listenerThreads) must not exceed this.
     */
    private long aggregateMemoryCeilingBytes = 1_073_741_824L; // 1 GB default

    /**
     * Instance identifier, used in temp file paths and filenames.
     */
    private String instanceId;

    public List<BindingConfig> getBindings() {
        return bindings;
    }

    public void setBindings(List<BindingConfig> bindings) {
        this.bindings = bindings;
    }

    public int getMaxumsgs() {
        return maxumsgs;
    }

    public void setMaxumsgs(int maxumsgs) {
        this.maxumsgs = maxumsgs;
    }

    public long getAggregateMemoryCeilingBytes() {
        return aggregateMemoryCeilingBytes;
    }

    public void setAggregateMemoryCeilingBytes(long aggregateMemoryCeilingBytes) {
        this.aggregateMemoryCeilingBytes = aggregateMemoryCeilingBytes;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }
}
