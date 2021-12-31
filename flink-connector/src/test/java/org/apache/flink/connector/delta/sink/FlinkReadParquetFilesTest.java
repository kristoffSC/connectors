package org.apache.flink.connector.delta.sink;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.BoundedSplitEnumeratorProvider;
import io.delta.flink.source.DeltaColumnarRowInputFormatFactory;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.DeltaSourceSplit;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.connector.file.sink.StreamingExecutionFileSinkITCase;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;

@RunWith(Parameterized.class)
@Ignore
public class FlinkReadParquetFilesTest extends StreamingExecutionFileSinkITCase {

    private static final Map<String, CountDownLatch> LATCH_MAP = new ConcurrentHashMap<>();
    private String latchId;
    private String nonPartitionedDeltaTablePath;
    private String partitionedDeltaTablePath;

    @Parameterized.Parameters(
        name = "triggerFailover = {0}"
    )
    public static Collection<Object[]> params() {
        return Arrays.asList(
            new Object[]{false},
            new Object[]{true}
        );
    }

    @Before
    public void setup() {
        this.latchId = UUID.randomUUID().toString();
        LATCH_MAP.put(latchId, new CountDownLatch(1));
        try {
            nonPartitionedDeltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            partitionedDeltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

            DeltaSinkTestUtils.initTestForNonPartitionedTable(nonPartitionedDeltaTablePath);
            DeltaSinkTestUtils.initTestForPartitionedTable(partitionedDeltaTablePath);
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @After
    public void teardown() {
        LATCH_MAP.remove(latchId);
    }

    @Test
    public void testDeltaLog() {
        DeltaLog deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(),
            nonPartitionedDeltaTablePath);
        Snapshot snapshot = deltaLog.snapshot();

        List<AddFile> filesFromSnapshot = snapshot.getAllFiles();

        Iterator<VersionLog> changes = deltaLog.getChanges(snapshot.getVersion(), true);

        while ((changes.hasNext())) {
            List<Action> actions = changes.next().getActions();
            System.out.println(actions);
        }

        System.out.println(changes);
    }

    @Test
    public void testDeltaBoundedSourceWithPartitions() throws Exception {
        StreamExecutionEnvironment env = getTestStreamEnv();

        final LogicalType[] fieldTypes =
            new LogicalType[]{
                new CharType(), new CharType(), new IntType(), new CharType(), new CharType()
            };

        List<String> partitions = Arrays.asList("col1", "col2");

        ParquetColumnarRowInputFormat<DeltaSourceSplit> format =
            DeltaColumnarRowInputFormatFactory.createPartitionedFormat(
                DeltaSinkTestUtils.getHadoopConf(),
                RowType.of(fieldTypes, new String[]{"name", "surname", "age", "col1", "col2"}),
                partitions,
                500,
                false, true
            );

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(partitionedDeltaTablePath)),
            format, new BoundedSplitEnumeratorProvider(), DeltaSinkTestUtils.getHadoopConf());

        env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "file-source")
            .print();

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testDeltaBoundedSource() throws Exception {
        StreamExecutionEnvironment env = getTestStreamEnv();

        final LogicalType[] fieldTypes =
            new LogicalType[]{
                new CharType(), new CharType(), new IntType()
            };

        ParquetColumnarRowInputFormat<DeltaSourceSplit> format =
            DeltaColumnarRowInputFormatFactory.createFormat(
                DeltaSinkTestUtils.getHadoopConf(),
                RowType.of(fieldTypes, new String[]{"name", "surname", "age"}),
                500,
                false, true
            );

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(nonPartitionedDeltaTablePath)),
            format, new BoundedSplitEnumeratorProvider(), DeltaSinkTestUtils.getHadoopConf());

        env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "file-source")
            .print();

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testFileSourceWithPartition() throws Exception {
        StreamExecutionEnvironment env = getTestStreamEnv();

        final LogicalType[] fieldTypes =
            new LogicalType[]{
                new CharType(), new CharType(), new IntType(), new CharType()
            };

        List<String> partitions = new ArrayList<>();
        partitions.add("col");

        ParquetColumnarRowInputFormat<FileSourceSplit> partitionedFormat =
            ParquetColumnarRowInputFormat.createPartitionedFormat(
                DeltaSinkTestUtils.getHadoopConf(),
                RowType.of(fieldTypes, new String[]{"name", "surname", "age", "col"}),
                partitions,
                (PartitionFieldExtractor<FileSourceSplit>) (split, fieldName, fieldType) ->
                    "partition_value",
                500,
                false, true
            );

        final FileSource<RowData> source =
            FileSource.forBulkFileFormat(partitionedFormat, Path.fromLocalFile(new File(
                    partitionedDeltaTablePath)))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source")
            .print();

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testFileSource() throws Exception {
        StreamExecutionEnvironment env = getTestStreamEnv();

        final LogicalType[] fieldTypes =
            new LogicalType[]{
                new CharType(), new CharType(), new IntType()
            };

        final ParquetColumnarRowInputFormat<FileSourceSplit> format =
            new ParquetColumnarRowInputFormat<>(
                DeltaSinkTestUtils.getHadoopConf(),
                RowType.of(fieldTypes, new String[]{"name", "surname", "age"}),
                500,
                false,
                true);

        final FileSource<RowData> source =
            FileSource.forBulkFileFormat(format, Path.fromLocalFile(new File(
                    nonPartitionedDeltaTablePath)))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source")
            .print();

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        try (MiniCluster miniCluster = DeltaSinkTestUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        org.apache.flink.configuration.Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);

        if (triggerFailover) {
            env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(1, Time.milliseconds(100)));
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }

        return env;
    }
}
