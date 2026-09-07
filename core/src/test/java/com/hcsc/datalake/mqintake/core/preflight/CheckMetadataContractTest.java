package com.hcsc.datalake.mqintake.core.preflight;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import javax.jms.Message;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The metadata every check carries, across all of them at once.
 *
 * <p>Each check answers three questions before it runs: which group it belongs
 * to, what it is called, and what a pass proves. Nothing held any of them —
 * changing an mq check's group to "hdfs", dropping the binding prefix from a
 * name, and returning the name from {@code describes()} all left the preflight
 * suite green.
 *
 * <p>The group is the one that bites. It is what {@code --preflight=<group>}
 * filters on, so a group that drifts means the check silently does not run
 * under that filter — and the narrowed run still reports success, which is the
 * worst way for a probe to fail.
 *
 * <p>Written across every check rather than per check on purpose: the point is
 * that the contract holds for all fifteen, including ones added later.
 */
class CheckMetadataContractTest {

    private static final Set<String> KNOWN_GROUPS = Set.of("mq", "hdfs", "app");

    @Test
    void everyCheckDeclaresAKnownGroup() {
        assertThat(allChecks()).allSatisfy(check ->
                assertThat(check.group())
                        .as("check '%s' must belong to a group the runner filters on", check.name())
                        .isIn(KNOWN_GROUPS));
    }

    @Test
    void everyGroupContainsOnlyItsOwnChecks() {
        // The filter is exact-match on group(), so a check in the wrong group
        // is skipped by `preflight mq` and the run still says PASS.
        assertThat(mqChecks()).allSatisfy(c -> assertThat(c.group()).isEqualTo("mq"));
        assertThat(hdfsChecks()).allSatisfy(c -> assertThat(c.group()).isEqualTo("hdfs"));
        assertThat(appChecks()).allSatisfy(c -> assertThat(c.group()).isEqualTo("app"));
    }

    @Test
    void everyCheckNameIsUniqueSoAReportLineIdentifiesOneCheck() {
        List<String> names = allChecks().stream()
                .map(PreflightCheck::name)
                .collect(Collectors.toList());

        assertThat(names).doesNotHaveDuplicates();
    }

    @Test
    void aPerBindingCheckCarriesItsBindingInItsName() {
        // Two bindings produce the same checks; without the prefix an operator
        // cannot tell which binding a failing line refers to.
        List<String> mqNames = mqChecks().stream()
                .map(PreflightCheck::name)
                .collect(Collectors.toList());

        assertThat(mqNames).allSatisfy(name ->
                assertThat(name).matches("^(rms|claims)\\..+"));
        assertThat(mqNames).anyMatch(n -> n.startsWith("rms."));
        assertThat(mqNames).anyMatch(n -> n.startsWith("claims."));
    }

    @Test
    void everyCheckDescribesWhatAPassProvesRatherThanRepeatingItsName() {
        assertThat(allChecks()).allSatisfy(check -> {
            assertThat(check.describes())
                    .as("check '%s' must say what a pass proves", check.name())
                    .isNotBlank();
            assertThat(check.describes())
                    .as("check '%s' describes itself with its own name", check.name())
                    .isNotEqualTo(check.name());
        });
    }

    // --- harness ---

    private List<PreflightCheck> allChecks() {
        List<PreflightCheck> all = new ArrayList<>();
        all.addAll(mqChecks());
        all.addAll(hdfsChecks());
        all.addAll(appChecks());
        return all;
    }

    private List<PreflightCheck> mqChecks() {
        return MqChecks.forAllBindings(properties(), mock(MqConnectionManager.class));
    }

    private List<PreflightCheck> hdfsChecks() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "file:///");
            return HdfsChecks.forAllBindings(properties(), FileSystem.get(conf), "test-instance");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private List<PreflightCheck> appChecks() {
        return AppChecks.forAllBindings(properties(), ProductionMode.disabled(),
                config -> TRIVIAL, null, true);
    }

    private static final RecordSerializer TRIVIAL = new RecordSerializer() {
        @Override
        public SerializedRecord serialize(Message message, RecordMetadata metadata) {
            return new SerializedRecord(new LongWritable(0), new Text(""));
        }

        @Override
        public Class<? extends Writable> getKeyClass() {
            return LongWritable.class;
        }

        @Override
        public Class<? extends Writable> getValueClass() {
            return Text.class;
        }
    };

    private IntakeProperties properties() {
        IntakeProperties props = new IntakeProperties();
        props.setInstanceId("preflight-test");
        props.getHdfs().setAuditBasePath("/tmp/preflight-test-audit");

        Map<String, MqConnectionConfig> connections = new LinkedHashMap<>();
        MqConnectionConfig primary = new MqConnectionConfig();
        primary.setId("primary");
        primary.setHost("test-host");
        primary.setQueueManager("QM1");
        primary.setChannel("TEST.SVRCONN");
        connections.put("primary", primary);
        props.setMqConnections(connections);

        List<BindingConfig> bindings = new ArrayList<>();
        for (String id : List.of("rms", "claims")) {
            BindingConfig binding = new BindingConfig();
            binding.setId(id);
            binding.setMqConnection("primary");
            binding.setMode(BindingMode.LAND_ONLY);
            binding.setSourceQueue("Q." + id.toUpperCase());
            binding.getBackout().setQueue("Q." + id.toUpperCase() + ".BOQ");
            binding.getHdfs().setBasePath("/tmp/preflight-test/" + id);
            binding.getBatch().setSize(10);
            binding.getBatch().setBytes(1024);
            binding.setListenerThreads(1);
            bindings.add(binding);
        }
        props.setBindings(bindings);
        return props;
    }
}
