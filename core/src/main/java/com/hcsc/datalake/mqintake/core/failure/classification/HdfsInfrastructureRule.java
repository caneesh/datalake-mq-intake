package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;

import java.io.IOException;

import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.anyType;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.classNameContains;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.messageContains;

/**
 * The storage layer is unavailable or refusing writes.
 *
 * <p>Never triggers degraded mode: shrinking the batch does not help an outage,
 * it just drives the same failure through more transactions.
 */
public final class HdfsInfrastructureRule extends MatcherFailureRule {

    public HdfsInfrastructureRule() {
        super(FailureClass.HDFS_INFRASTRUCTURE,
                // Hadoop's own exception types, minus the security ones, which
                // SecurityConfigRule has already claimed by this point.
                classNameContains("hadoop", "hdfs")
                        .and(classNameContains("Security", "Access").negate())
                .or(anyType(IOException.class)
                        .and(messageContains("NameNode", "DataNode", "HDFS", "quota",
                                "block", "replication", "lease", "safemode")))
                // Qualified with the Hadoop package: bare "FileSystem" and
                // "RemoteException" also match java.nio.file and java.rmi
                // types that have nothing to do with HDFS.
                .or(classNameContains("org.apache.hadoop.fs.", "org.apache.hadoop.ipc.")),
                true);
    }
}
