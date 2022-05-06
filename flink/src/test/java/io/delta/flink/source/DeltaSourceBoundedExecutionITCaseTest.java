package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.DeltaSinkInternal;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.sink.utils.TestParquetReader;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public class DeltaSourceBoundedExecutionITCaseTest extends DeltaSourceITBase {

    @BeforeAll
    public static void beforeAll() throws IOException {
        DeltaSourceITBase.beforeAll();
    }

    @AfterAll
    public static void afterAll() {
        DeltaSourceITBase.afterAll();
    }

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @AfterEach
    public void after() {
        super.after();
    }

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
    @EnumSource(FailoverType.class)
    // NOTE that this test can take some time to finish since we are restarting JM here.
    // It can be around 30 seconds or so.
    // Test if SplitEnumerator::addSplitsBack works well,
    // meaning if splits were added back to the Enumerator's state and reassigned to new TM.
    public void shouldReadDeltaTable(FailoverType failoverType) throws Exception {

        DeltaSource<RowData> deltaSource =
            initBoundedSource(
                nonPartitionedLargeTablePath,
                LARGE_TABLE_COLUMN_NAMES,
                LARGE_TABLE_COLUMN_TYPES);

        // WHEN
        // Fail TaskManager or JobManager after half of the records or do not fail anything if
        // FailoverType.NONE.
        List<RowData> resultData = testBoundDeltaSource(failoverType, deltaSource,
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2);

        Set<Long> actualValues =
            resultData.stream().map(row -> row.getLong(0)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta table have.", resultData.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
        assertThat("Source Must Have produced some duplicates.", actualValues.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
    }

    @Test
    public void endToEnd() throws Exception {
        DeltaSource<RowData> deltaSource =
            initBoundedSource(
                nonPartitionedLargeTablePath,
                LARGE_TABLE_COLUMN_NAMES,
                LARGE_TABLE_COLUMN_TYPES);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        //env.enableCheckpointing(100);

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");

        String sinkPath = TMP_FOLDER.newFolder().getAbsolutePath();

        RowType rowType = RowType.of(LARGE_TABLE_COLUMN_TYPES, LARGE_TABLE_COLUMN_NAMES);
        DeltaSinkInternal<RowData> deltaSink = DeltaSinkTestUtils
            .createDeltaSink(sinkPath, rowType, false);
        stream.sinkTo(deltaSink);

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        Snapshot snapshot = verifyDeltaTable(sinkPath, rowType);

        CloseableIterator<RowRecord> iter = snapshot.open();
        RowRecord row;
        while (iter.hasNext()) {
            row = iter.next();
            System.out.println(Arrays.toString(row.getSchema().getFieldNames()));
        }
        iter.close();
    }

    // for this test change RowDataFormatBuilder#PARQUET_CASE_SENSITIVE to false
    @Test
    public void endToEndMixedColumnNameCase() throws Exception {
        String sinkPath = TMP_FOLDER.newFolder().getAbsolutePath();
        RowType rowType = RowType.of(LARGE_TABLE_COLUMN_TYPES, LARGE_TABLE_COLUMN_NAMES);
        RowType rowTypeCase = RowType.of(LARGE_TABLE_COLUMN_TYPES, LARGE_TABLE_COLUMN_NAMES_CASE);

        StreamExecutionEnvironment env1 =
            getStreamExecutionEnvironment(sinkPath, rowType);
        StreamExecutionEnvironment env2 =
            getStreamExecutionEnvironment(sinkPath, rowTypeCase);

        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(env1.getStreamGraph().getJobGraph());
            miniCluster.executeJobBlocking(env2.getStreamGraph().getJobGraph());
        }

        verifyDeltaTable(sinkPath, rowType);
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
        String sinkPath,
        RowType rowType) {

        DeltaSource<RowData> deltaSource =
            initBoundedSource(
                nonPartitionedLargeTablePath,
                LARGE_TABLE_COLUMN_NAMES,
                LARGE_TABLE_COLUMN_TYPES);

        DeltaSinkInternal<RowData> deltaSink = DeltaSink
            .forRowData(
                new Path(sinkPath),
                DeltaSinkTestUtils.getHadoopConf(),
                rowType).withMergeSchema(true).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        //env.enableCheckpointing(100);

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);
        return env;
    }

    private Snapshot verifyDeltaTable(String sinkPath, RowType rowType) throws IOException {
        DeltaLog deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(), sinkPath);
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

    private DeltaSource<RowData> initBoundedSource(
        String tablePath, String[] columnNames, LogicalType[] columnTypes) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        return DeltaSource.forBoundedRowData(
            Path.fromLocalFile(new File(tablePath)),
            columnNames,
            columnTypes,
            hadoopConf
        ).build();
    }

    @Override
    protected List<RowData> testWithPartitions(DeltaSource<RowData> deltaSource) throws Exception {
        return testBoundedDeltaSource(deltaSource);
    }

    @Override
    protected DeltaSource<RowData> initPartitionedSource(
        String tablePath,
        String[] columnNames,
        LogicalType[] columnTypes,
        List<String> partitionColumns) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        return DeltaSource.forBoundedRowData(
                Path.fromLocalFile(new File(tablePath)),
                columnNames,
                columnTypes,
                hadoopConf
            ).partitionColumns(partitionColumns)
            .build();
    }
}
