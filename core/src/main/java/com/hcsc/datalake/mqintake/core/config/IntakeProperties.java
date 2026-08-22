package com.hcsc.datalake.mqintake.core.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Root configuration properties for the MQ intake service.
 */
@ConfigurationProperties(prefix = "intake")
public class IntakeProperties {

    private List<BindingConfig> bindings = new ArrayList<>();
    private Map<String, MqConnectionConfig> mqConnections = new HashMap<>();
    private int maxumsgs = 10_000;
    private long aggregateMemoryCeilingBytes = 1_073_741_824L;
    private String instanceId;

    private MqProperties mq = new MqProperties();
    private KerberosProperties kerberos = new KerberosProperties();
    private HdfsProperties hdfs = new HdfsProperties();
    private ShutdownProperties shutdown = new ShutdownProperties();

    public List<BindingConfig> getBindings() {
        return bindings;
    }

    public void setBindings(List<BindingConfig> bindings) {
        this.bindings = bindings;
    }

    public Map<String, MqConnectionConfig> getMqConnections() {
        return mqConnections;
    }

    public void setMqConnections(Map<String, MqConnectionConfig> mqConnections) {
        this.mqConnections = mqConnections;
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

    public MqProperties getMq() {
        return mq;
    }

    public void setMq(MqProperties mq) {
        this.mq = mq;
    }

    public KerberosProperties getKerberos() {
        return kerberos;
    }

    public void setKerberos(KerberosProperties kerberos) {
        this.kerberos = kerberos;
    }

    public HdfsProperties getHdfs() {
        return hdfs;
    }

    public void setHdfs(HdfsProperties hdfs) {
        this.hdfs = hdfs;
    }

    public ShutdownProperties getShutdown() {
        return shutdown;
    }

    public void setShutdown(ShutdownProperties shutdown) {
        this.shutdown = shutdown;
    }

    /**
     * IBM MQ connection properties.
     */
    public static class MqProperties {
        private String host;
        private int port = 1414;
        private String queueManager;
        private String channel;
        private String user;
        private String password;
        private long receiveTimeoutMs = 1000;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getQueueManager() { return queueManager; }
        public void setQueueManager(String queueManager) { this.queueManager = queueManager; }
        public String getChannel() { return channel; }
        public void setChannel(String channel) { this.channel = channel; }
        public String getUser() { return user; }
        public void setUser(String user) { this.user = user; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public long getReceiveTimeoutMs() { return receiveTimeoutMs; }
        public void setReceiveTimeoutMs(long receiveTimeoutMs) { this.receiveTimeoutMs = receiveTimeoutMs; }
    }

    /**
     * Kerberos authentication properties.
     */
    public static class KerberosProperties {
        private boolean enabled = false;
        private String principal;
        private String keytabPath;
        private long reloginIntervalMs = 3600000; // 1 hour

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getPrincipal() { return principal; }
        public void setPrincipal(String principal) { this.principal = principal; }
        public String getKeytabPath() { return keytabPath; }
        public void setKeytabPath(String keytabPath) { this.keytabPath = keytabPath; }
        public long getReloginIntervalMs() { return reloginIntervalMs; }
        public void setReloginIntervalMs(long reloginIntervalMs) { this.reloginIntervalMs = reloginIntervalMs; }
    }

    /**
     * HDFS properties.
     */
    public static class HdfsProperties {
        private String auditBasePath = "/data/audit";
        private long tempFileMaxAgeMs = 3600000; // 1 hour

        public String getAuditBasePath() { return auditBasePath; }
        public void setAuditBasePath(String auditBasePath) { this.auditBasePath = auditBasePath; }
        public long getTempFileMaxAgeMs() { return tempFileMaxAgeMs; }
        public void setTempFileMaxAgeMs(long tempFileMaxAgeMs) { this.tempFileMaxAgeMs = tempFileMaxAgeMs; }
    }

    /**
     * Shutdown properties.
     */
    public static class ShutdownProperties {
        private long drainTimeoutMs = 30000; // 30 seconds

        public long getDrainTimeoutMs() { return drainTimeoutMs; }
        public void setDrainTimeoutMs(long drainTimeoutMs) { this.drainTimeoutMs = drainTimeoutMs; }
    }
}
