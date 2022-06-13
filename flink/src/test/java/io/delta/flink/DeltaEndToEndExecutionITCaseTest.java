package io.delta.flink;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.DeltaSinkInternal;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.utils.ContinuousTestDescriptor;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.FailoverType;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import io.delta.flink.utils.TableUpdateDescriptor;
import io.delta.flink.utils.TestParquetReader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.Path;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.DATA_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.DATA_COLUMN_TYPES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_TYPES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_RECORD_COUNT;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.SMALL_TABLE_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

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

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
    @EnumSource(FailoverType.class)
    public void endToEndBoundedStream(FailoverType failoverType) throws Exception {
        DeltaTestUtils.initTestForNonPartitionedLargeTable(sourceTablePath);

        DeltaSource<RowData> deltaSource = DeltaSource.forBoundedRowData(
                new Path(sourceTablePath),
                DeltaTestUtils.getHadoopConf()
            )
            .build();

        RowType rowType = RowType.of(LARGE_TABLE_ALL_COLUMN_TYPES, LARGE_TABLE_ALL_COLUMN_NAMES);
        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                new Path(sinkTablePath),
                DeltaTestUtils.getHadoopConf(),
                rowType)
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);

        DeltaTestUtils.testBoundedStream(
            failoverType,
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2,
            stream,
            miniClusterResource
        );

        verifyDeltaTable(sinkTablePath, rowType, LARGE_TABLE_RECORD_COUNT);
    }

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
    @EnumSource(FailoverType.class)
    public void endToEndUnBoundedStream(FailoverType failoverType) throws Exception {
        DeltaTestUtils.initTestForNonPartitionedTable(sourceTablePath);

        DeltaSource<RowData> deltaSource = DeltaSource.forContinuousRowData(
                new Path(sourceTablePath),
                DeltaTestUtils.getHadoopConf()
            )
            .build();

        RowType rowType = RowType.of(DATA_COLUMN_TYPES, DATA_COLUMN_NAMES);
        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                new Path(sinkTablePath),
                DeltaTestUtils.getHadoopConf(),
                rowType)
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        env.enableCheckpointing(100);

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);

        int numberOfTableUpdateBulks = 5;
        int rowsPerTableUpdate = 5;
        int expectedRowCount = SMALL_TABLE_COUNT + numberOfTableUpdateBulks * rowsPerTableUpdate;

        ContinuousTestDescriptor testDescriptor = DeltaTestUtils.prepareTableUpdates(
            deltaSource.getTablePath().toUri().toString(),
            RowType.of(DATA_COLUMN_TYPES, DATA_COLUMN_NAMES),
            SMALL_TABLE_COUNT,
            new TableUpdateDescriptor(numberOfTableUpdateBulks, rowsPerTableUpdate)
        );

        DeltaTestUtils.testContinuousStream(
            failoverType,
            testDescriptor,
            (FailCheck) readRows -> readRows == expectedRowCount/ 2,
            stream,
            miniClusterResource
        );

        verifyDeltaTable(sinkTablePath, rowType, expectedRowCount);
    }

    @SuppressWarnings("unchecked")
    private void verifyDeltaTable(
            String sinkPath,
            RowType rowType,
            Integer expectedNumberOfRecords) throws IOException {

        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), sinkPath);
        Snapshot snapshot = deltaLog.snapshot();
        CloseableIterator<RowRecord> open = snapshot.open();
        System.out.println(open);
        open.close();
        List<AddFile> deltaFiles = snapshot.getAllFiles();
        int finalTableRecordsCount = TestParquetReader
            .readAndValidateAllTableRecords(
                deltaLog,
                rowType,
                DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(rowType)));
        long finalVersion = snapshot.getVersion();

        LOG.info(
            String.format(
                "RESULTS: final record count: [%d], final table version: [%d], number of Delta "
                    + "files: [%d]",
                finalTableRecordsCount,
                finalVersion,
                deltaFiles.size()
            )
        );

        assertThat(finalTableRecordsCount, equalTo(expectedNumberOfRecords));
    }
}
