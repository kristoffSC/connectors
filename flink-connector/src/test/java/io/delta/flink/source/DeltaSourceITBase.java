package io.delta.flink.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class DeltaSourceITBase extends TestLogger {

    @ClassRule
    public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    protected static final int PARALLELISM = 4;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(PARALLELISM)
                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                .withHaLeadershipControl()
                .build());

    public static void triggerFailover(FailoverType type, JobID jobId, Runnable afterFailAction,
        MiniCluster miniCluster)
        throws Exception {
        switch (type) {
            case NONE:
                afterFailAction.run();
                break;
            case TM:
                restartTaskManager(afterFailAction, miniCluster);
                break;
            case JM:
                triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
                break;
        }
    }

    public static void triggerJobManagerFailover(
        JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    public static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
        throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    protected ParquetColumnarRowInputFormat<DeltaSourceSplit> buildPartitionedFormat(
        String[] columnNames, LogicalType[] columnTypes, String[] partitionKeys) {

        return DeltaColumnarRowInputFormatFactory.createPartitionedFormat(
            DeltaSinkTestUtils.getHadoopConf(),
            RowType.of(columnTypes, columnNames),
            Arrays.asList(partitionKeys),
            500,
            false, true
        );
    }

    protected ParquetColumnarRowInputFormat<DeltaSourceSplit> buildFormat(String[] columnNames,
        LogicalType[] columnTypes) {

        return DeltaColumnarRowInputFormatFactory.createFormat(
            DeltaSinkTestUtils.getHadoopConf(),
            RowType.of(columnTypes, columnNames),
            500,
            false, true
        );
    }

    protected <T> List<T> testDeltaSource(DeltaSource<T> source)
        throws Exception {
        return testDeltaSource(FailoverType.NONE, source, (FailCheck) integer -> true);
    }

    protected <T> List<T> testDeltaSource(FailoverType failoverType, DeltaSource<T> source,
        FailCheck failCheck) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        env.enableCheckpointing(10L);

        DataStream<T> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        DataStream<T> streamFailingInTheMiddleOfReading =
            RecordCounterToFail.wrapWithFailureAfter(stream, failCheck);

        ClientAndIterator<T> client =
            DataStreamUtils.collectWithClient(
                streamFailingInTheMiddleOfReading, "Bounded DeltaSource Test");
        JobID jobId = client.client.getJobID();

        RecordCounterToFail.waitToFail(client);
        triggerFailover(
            failoverType,
            jobId,
            RecordCounterToFail::continueProcessing,
            miniClusterResource.getMiniCluster());

        final List<T> result = new ArrayList<>();
        while (client.iterator.hasNext()) {
            result.add(client.iterator.next());
        }

        return result;
    }

    public enum FailoverType {
        NONE,
        TM,
        JM
    }

}
