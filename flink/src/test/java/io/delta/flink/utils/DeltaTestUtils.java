package io.delta.flink.utils;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

public class DeltaTestUtils {

    ///////////////////////////////////////////////////////////////////////////
    // hadoop conf test utils
    ///////////////////////////////////////////////////////////////////////////

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        conf.set("io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID", "UTC");
        return conf;
    }

    ///////////////////////////////////////////////////////////////////////////
    // test data utils
    ///////////////////////////////////////////////////////////////////////////

    public static final String TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR =
        "/test-data/test-non-partitioned-delta-table-initial-state";
    public static final String TEST_DELTA_TABLE_INITIAL_STATE_P_DIR =
        "/test-data/test-partitioned-delta-table-initial-state";
    public static final String TEST_DELTA_LARGE_TABLE_INITIAL_STATE_DIR =
        "/test-data/test-non-partitioned-delta-table_1100_records";
    public static final String TEST_PARTITIONED_DELTA_TABLE_SOURCE =
        "/test-data/test-partitioned-delta-table-source";

    public static void initTestForNonPartitionedTable(String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    public static void initTestForSourcePartitionedTable(String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + TEST_PARTITIONED_DELTA_TABLE_SOURCE;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    public static void initTestForPartitionedTable(String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + TEST_DELTA_TABLE_INITIAL_STATE_P_DIR;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    public static void initTestForNonPartitionedLargeTable(String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + TEST_DELTA_LARGE_TABLE_INITIAL_STATE_DIR;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    public static void triggerFailover(FailoverType type, JobID jobId, Runnable afterFailAction,
        MiniCluster miniCluster) throws Exception {
        switch (type) {
            case NONE:
                afterFailAction.run();
                break;
            case TASK_MANAGER:
                restartTaskManager(afterFailAction, miniCluster);
                break;
            case JOB_MANAGER:
                triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
                break;
        }
    }

    public static void triggerJobManagerFailover(
        JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        System.out.println("Triggering Job Manager failover.");
        HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    public static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
        throws Exception {
        System.out.println("Triggering Task Manager failover.");
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    public static MiniClusterWithClientResource buildCluster(int parallelismLevel) {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        return new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(parallelismLevel)
                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                .withHaLeadershipControl()
                .setConfiguration(configuration)
                .build());
    }

}
