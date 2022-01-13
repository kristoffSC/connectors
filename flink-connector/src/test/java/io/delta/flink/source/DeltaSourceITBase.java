package io.delta.flink.source;

import java.util.ArrayList;
import java.util.List;

import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
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
    public final MiniClusterWithClientResource miniClusterResource = buildCluster();

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

    private MiniClusterWithClientResource buildCluster() {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        return new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(PARALLELISM)
                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                .withHaLeadershipControl()
                .setConfiguration(configuration)
                .build());
    }

    protected <T> List<T> testBoundDeltaSource(DeltaSource<T> source)
        throws Exception {
        return testBoundDeltaSource(FailoverType.NONE, source, (FailCheck) integer -> true);
    }

    protected <T> List<T> testBoundDeltaSource(FailoverType failoverType, DeltaSource<T> source,
        FailCheck failCheck) throws Exception {

        if (source.getBoundedness() == Boundedness.CONTINUOUS_UNBOUNDED) {
            throw new RuntimeException(
                "Using Continuous source in Bounded test setup. This will not work properly.");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<T> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        DataStream<T> streamFailingInTheMiddleOfReading =
            RecordCounterToFail.wrapWithFailureAfter(stream, failCheck);

        ClientAndIterator<T> client =
            DataStreamUtils.collectWithClient(
                streamFailingInTheMiddleOfReading, "Bounded DeltaSource Test");
        JobID jobId = client.client.getJobID();

        RecordCounterToFail.waitToFail();
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

    protected <T> List<List<T>> testContinuousDeltaSource(
        DeltaSource<T> source, ContinuousTestDescriptor testDescriptor)
        throws Exception {
        return testContinuousDeltaSource(FailoverType.NONE, source, testDescriptor,
            (FailCheck) integer -> true);
    }

    protected <T> List<List<T>> testContinuousDeltaSource(
        FailoverType failoverType, DeltaSource<T> source, ContinuousTestDescriptor testDescriptor,
        FailCheck failCheck)
        throws Exception {

        DeltaTableUpdater tableUpdater = new DeltaTableUpdater(source.getTablePath().toString());
        List<List<T>> totalResults = new ArrayList<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(10L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<T> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        ClientAndIterator<T> client =
            DataStreamUtils.collectWithClient(stream, "Continuous DeltaSource  Test");
        JobID jobId = client.client.getJobID();

        // Initial Table Data
        totalResults.add(DataStreamUtils.collectRecordsFromUnboundedStream(client,
            testDescriptor.getInitialDataSize()));

        // Table Updates
        testDescriptor.getUpdateDescriptors().forEach(descriptor -> {
            tableUpdater.writeToTable(descriptor);
            totalResults.add(DataStreamUtils.collectRecordsFromUnboundedStream(client,
                descriptor.getExpectedCount()));
            System.out.println(totalResults.size());
        });

        client.client.cancel().get();

        return totalResults;

        /*        // write the remaining files over time, after that collect the final result
        for (int i = 1; i < LINES_PER_FILE.length; i++) {
            Thread.sleep(10);
            writeFile(testDir, i);
            final boolean failAfterHalfOfInput = i == LINES_PER_FILE.length / 2;
            if (failAfterHalfOfInput) {
                triggerFailover(type, jobId, () -> {
                }, miniClusterResource.getMiniCluster());
            }
        }

        final List<String> result2 =
            DataStreamUtils.collectRecordsFromUnboundedStream(client, numLinesAfter);


        result1.addAll(result2);*/

        // shut down the job, now that we have all the results we expected.

    }

    public enum FailoverType {
        NONE,
        TM,
        JM
    }

}
