package io.delta.flink;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.DeltaSinkInternal;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.TestParquetReader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_TYPES;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public class DeltaEndToEndExecutionITCaseTest {

    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaEndToEndExecutionITCaseTest.class);

    private static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    private static final int PARALLELISM = 4;

    private final ExecutorService singleThreadExecutor =
        Executors.newSingleThreadExecutor();

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    private String sourceTablePath;

    private String sinkTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TMP_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TMP_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() {
        try {
            miniClusterResource.before();

            sourceTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            sinkTablePath = TMP_FOLDER.newFolder().getAbsolutePath();

        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @Test
    public void endToEndBoundedSource() throws Exception {
        DeltaTestUtils.initTestForNonPartitionedLargeTable(sourceTablePath);

        DeltaSource<RowData> deltaSource = DeltaSource.forBoundedRowData(
                new Path(sourceTablePath),
                DeltaTestUtils.getHadoopConf()
            )
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        env.enableCheckpointing(100);

        RowType rowType = RowType.of(LARGE_TABLE_ALL_COLUMN_TYPES, LARGE_TABLE_ALL_COLUMN_NAMES);
        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                new Path(sinkTablePath),
                DeltaTestUtils.getHadoopConf(),
                rowType)
            .build();

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        Snapshot snapshot = verifyDeltaTable(sinkTablePath, rowType);

        CloseableIterator<RowRecord> iter = snapshot.open();
        RowRecord row;
        while (iter.hasNext()) {
            row = iter.next();
            LOG.info(Arrays.toString(row.getSchema().getFieldNames()));
        }
        iter.close();
    }

    @SuppressWarnings("unchecked")
    private Snapshot verifyDeltaTable(String sinkPath, RowType rowType) throws IOException {
        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), sinkPath);
        Snapshot snapshot = deltaLog.snapshot();
        List<AddFile> deltaFiles = snapshot.getAllFiles();
        int finalTableRecordsCount = TestParquetReader
            .readAndValidateAllTableRecords(
                deltaLog,
                rowType,
                DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(rowType)));
        long finalVersion = snapshot.getVersion();

        // Assert This
        System.out.println(
            "RESULTS: " + String.join(
                ",",
                String.valueOf(finalTableRecordsCount),
                String.valueOf(finalVersion),
                String.valueOf(deltaFiles.size()))
        );
        return snapshot;
    }

}
