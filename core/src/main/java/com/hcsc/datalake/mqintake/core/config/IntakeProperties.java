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
    /**
     * Ceiling on the summed in-memory batch budget across all bindings.
     *
     * <p>Leave unset (0) to derive it from the JVM's actual max heap, which is
     * almost always what you want: a fixed byte count has no relationship to
     * the heap the process was given, so it can approve a configuration that is
     * certain to OOM, or reject one that is perfectly safe.
     */
    private long aggregateMemoryCeilingBytes = 0L;
    private String instanceId;

    private KerberosProperties kerberos = new KerberosProperties();
    private HdfsProperties hdfs = new HdfsProperties();
    private ShutdownProperties shutdown = new ShutdownProperties();
    private ReconciliationProperties reconciliation = new ReconciliationProperties();
    private PreflightProperties preflight = new PreflightProperties();

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

    public PreflightProperties getPreflight() {
        return preflight;
    }

    public void setPreflight(PreflightProperties preflight) {
        this.preflight = preflight;
    }

    /**
     * Diagnostic mode: probe each dependency, print a report, exit.
     *
     * <p>When enabled the runtime does not auto-start, so no listener thread
     * is created and nothing is consumed — preflight is safe to run against
     * an environment carrying live data.
     */
    public static class PreflightProperties {

        private boolean enabled = false;

        /** Groups to run — {@code mq}, {@code hdfs}, {@code app}; empty = all. */
        private java.util.List<String> only = new java.util.ArrayList<>();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public java.util.List<String> getOnly() {
            return only;
        }

        public void setOnly(java.util.List<String> only) {
            this.only = only;
        }
    }

    public ReconciliationProperties getReconciliation() {
        return reconciliation;
    }

    public void setReconciliation(ReconciliationProperties reconciliation) {
        this.reconciliation = reconciliation;
    }

    public ShutdownProperties getShutdown() {
        return shutdown;
    }

    public void setShutdown(ShutdownProperties shutdown) {
        this.shutdown = shutdown;
    }

    /**
     * Periodic reconciliation — the check half of ABC.
     *
     * <p>The audit records say what should be on HDFS; reconciliation is what
     * confirms it actually is. Without it the audit is evidence nobody reads.
     */
    public static class ReconciliationProperties {

        /**
         * Off by default so enabling it is a deliberate act, and stated
         * explicitly in each application's configuration rather than inherited
         * from a hidden default.
         */
        private boolean enabled = false;

        /** How often to run. Defaults to one partition window. */
        private long intervalMs = 900_000;

        /**
         * How long after a window closes before reconciling it.
         *
         * <p>A partition is still being written to right up to its boundary,
         * and a batch that started before the boundary lands after it. Checking
         * too early reports work in flight as a discrepancy.
         */
        private long gracePeriodMs = 300_000;

        /**
         * How many closed windows each run examines.
         *
         * <p>More than one so a run that is skipped — overlap, restart, a
         * transient HDFS problem — does not leave a window permanently
         * unchecked.
         */
        private int lookbackWindows = 4;

        /**
         * Whether duplicate-classified orphans are moved to _quarantine.
         *
         * <p>Off by default: a move is still a change to landed data, and for
         * a feed that cannot lose messages the safe first posture is to report
         * and let a human decide.
         */
        private boolean quarantineDuplicates = false;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }
        public long getGracePeriodMs() { return gracePeriodMs; }
        public void setGracePeriodMs(long gracePeriodMs) { this.gracePeriodMs = gracePeriodMs; }
        public int getLookbackWindows() { return lookbackWindows; }
        public void setLookbackWindows(int lookbackWindows) { this.lookbackWindows = lookbackWindows; }
        public boolean isQuarantineDuplicates() { return quarantineDuplicates; }
        public void setQuarantineDuplicates(boolean q) { this.quarantineDuplicates = q; }
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

        /**
         * Cluster configuration to load: {@code core-site.xml} /
         * {@code hdfs-site.xml} files, or directories containing them
         * (typically {@code /etc/hadoop/conf}).
         *
         * <p>Required to reach a real cluster. Hadoop finds these on the
         * classpath, and a Spring Boot fat jar started with {@code java -jar}
         * has only itself on the classpath — so without this the service falls
         * back to Hadoop's default {@code fs.defaultFS=file:///} and writes to
         * the <em>local disk of the server</em>, silently and successfully.
         * That is the worst kind of misconfiguration: everything reports
         * healthy while the data is nowhere anyone will look for it.
         */
        private java.util.List<String> configResources = new java.util.ArrayList<>();

        /**
         * Permits a local (non-distributed) filesystem in production mode.
         *
         * <p>Off by default, which makes production mode refuse to start on
         * {@code file:///} — see {@link #configResources}. Tests that
         * deliberately run the production profile against a temporary
         * directory set this true.
         */
        private boolean allowLocalFilesystem = false;

        public String getAuditBasePath() { return auditBasePath; }
        public void setAuditBasePath(String auditBasePath) { this.auditBasePath = auditBasePath; }
        public long getTempFileMaxAgeMs() { return tempFileMaxAgeMs; }
        public void setTempFileMaxAgeMs(long tempFileMaxAgeMs) { this.tempFileMaxAgeMs = tempFileMaxAgeMs; }
        public java.util.List<String> getConfigResources() { return configResources; }
        public void setConfigResources(java.util.List<String> configResources) {
            this.configResources = configResources;
        }
        public boolean isAllowLocalFilesystem() { return allowLocalFilesystem; }
        public void setAllowLocalFilesystem(boolean allowLocalFilesystem) {
            this.allowLocalFilesystem = allowLocalFilesystem;
        }
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
