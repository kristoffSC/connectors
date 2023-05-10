package io.delta.flink;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.DeltaSinkInternal;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.FailoverType;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import io.delta.flink.utils.TableUpdateDescriptor;
import io.delta.flink.utils.TestDescriptor;
import io.delta.flink.utils.resources.AllTypesNonPartitionedTableInfo;
import io.delta.flink.utils.resources.LargeNonPartitionedTableInfo;
import io.delta.flink.utils.resources.NonPartitionedTableInfo;
import io.delta.flink.utils.resources.TableInfo;
import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static io.delta.flink.utils.DeltaTestUtils.verifyDeltaTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public class DeltaEndToEndExecutionITCaseTest {

    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaEndToEndExecutionITCaseTest.class);

    private static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    private static final int PARALLELISM = 4;

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

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
            sinkTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L,
        repeats = 3,
        name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    public void testEndToEndBoundedStream(FailoverType failoverType) throws Exception {
        TableInfo sourceTableInfo = LargeNonPartitionedTableInfo.create(TMP_FOLDER);

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConfiguration = DeltaTestUtils.getConfigurationWithMockFs();

        Path sourceTablePath = Path.fromLocalFile(new File(sourceTableInfo.getTablePath()));
        Path sinkTablePath = Path.fromLocalFile(new File(this.sinkTablePath));

        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));
        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));

        DeltaSource<RowData> deltaSource = DeltaSource.forBoundedRowData(
                sourceTablePath,
                hadoopConfiguration
            )
            .build();

        RowType rowType = sourceTableInfo.getRowType();
        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                sinkTablePath,
                hadoopConfiguration,
                rowType)
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);

        // The tableInfo.getInitialRecordCount(); cannot be passed directly to FailCheck lambda
        // since TableInfo is not serializable.
        int initialRecordCount = sourceTableInfo.getInitialRecordCount();
        DeltaTestUtils.testBoundedStream(
            failoverType,
            (FailCheck) readRows -> readRows == initialRecordCount / 2,
            stream,
            miniClusterResource
        );

        verifyDeltaTable(sourceTableInfo);
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L,
        repeats = 3,
        name = "{index}: FailoverType = [{0}]"
    )
    @EnumSource(FailoverType.class)
    public void testEndToEndContinuousStream(FailoverType failoverType) throws Exception {
        TableInfo sourceTableInfo = NonPartitionedTableInfo.createWithInitData(TMP_FOLDER);

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConfiguration = DeltaTestUtils.getConfigurationWithMockFs();

        Path sourceTablePath = Path.fromLocalFile(new File(sourceTableInfo.getTablePath()));
        Path sinkTablePath = Path.fromLocalFile(new File(this.sinkTablePath));

        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));
        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));

        DeltaSource<RowData> deltaSource = DeltaSource.forContinuousRowData(
                sourceTablePath,
                hadoopConfiguration
            )
            .build();

        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                sinkTablePath,
                hadoopConfiguration,
                sourceTableInfo.getRowType())
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
        int expectedRowCount =
            sourceTableInfo.getInitialRecordCount() + numberOfTableUpdateBulks * rowsPerTableUpdate;

        TestDescriptor testDescriptor = DeltaTestUtils.prepareTableUpdates(
            sourceTableInfo,
            new TableUpdateDescriptor(numberOfTableUpdateBulks, rowsPerTableUpdate)
        );

        DeltaTestUtils.testContinuousStream(
            failoverType,
            testDescriptor,
            (FailCheck) readRows -> readRows == expectedRowCount / 2,
            stream,
            miniClusterResource
        );

        verifyDeltaTable(sourceTableInfo, expectedRowCount);
    }

    @RepeatedIfExceptionsTest(suspend = 2000L, repeats = 3)
    public void testEndToEndReadAllDataTypes() throws Exception {

        // this test uses test-non-partitioned-delta-table-alltypes table. See README.md from
        // table's folder for detail information about this table.
        TableInfo sourceTableInfo = AllTypesNonPartitionedTableInfo.create(TMP_FOLDER);

        // Making sure that we are using path with schema to file system "file://"
        Configuration hadoopConfiguration = DeltaTestUtils.getConfigurationWithMockFs();

        Path sourceTablePath = Path.fromLocalFile(new File(sourceTableInfo.getTablePath()));
        Path sinkTablePath = Path.fromLocalFile(new File(this.sinkTablePath));

        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));
        assertThat(sinkTablePath.toUri().getScheme(), equalTo("file"));

        DeltaSource<RowData> deltaSource = DeltaSource.forBoundedRowData(
                sourceTablePath,
                hadoopConfiguration
            )
            .build();

        RowType rowType = sourceTableInfo.getRowType();
        DeltaSinkInternal<RowData> deltaSink = DeltaSink.forRowData(
                sinkTablePath,
                hadoopConfiguration,
                rowType)
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<RowData> stream =
            env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");
        stream.sinkTo(deltaSink);

        DeltaTestUtils.testBoundedStream(stream, miniClusterResource);

        Snapshot snapshot = verifyDeltaTable(sourceTableInfo);

        assertRowsFromSnapshot(snapshot, sourceTableInfo);
    }

    /**
     * Read entire snapshot using delta standalone and check every column.
     *
     * @param snapshot {@link Snapshot} to read data from.
     */
    private void assertRowsFromSnapshot(Snapshot snapshot, TableInfo tableInfo) throws IOException {

        final AtomicInteger index = new AtomicInteger(0);
        try (CloseableIterator<RowRecord> iterator = snapshot.open()) {
            while (iterator.hasNext()) {
                final int i = index.getAndIncrement();

                BigDecimal expectedBigDecimal = BigDecimal.valueOf((double) i).setScale(18);

                RowRecord row = iterator.next();
                LOG.info("Row Content: " + row.toString());
                String[] columnNames = tableInfo.getDataColumnNames();
                assertAll(() -> {
                        assertThat(
                            row.getByte(columnNames[0]),
                            equalTo(new Integer(i).byteValue())
                        );
                        assertThat(
                            row.getShort(columnNames[1]),
                            equalTo((short) i)
                        );
                        assertThat(row.getInt(columnNames[2]), equalTo(i));
                        assertThat(
                            row.getDouble(columnNames[3]),
                            equalTo(new Integer(i).doubleValue())
                        );
                        assertThat(
                            row.getFloat(columnNames[4]),
                            equalTo(new Integer(i).floatValue())
                        );

                        // In Source Table this column was generated as: BigInt(x)
                        assertThat(
                            row.getBigDecimal(columnNames[5]),
                            equalTo(expectedBigDecimal)
                        );

                        assertThat(
                            row.getBigDecimal(columnNames[6]),
                            equalTo(expectedBigDecimal)
                        );

                        // same value for all columns
                        assertThat(
                            row.getTimestamp(columnNames[7])
                                .toLocalDateTime().toInstant(ZoneOffset.UTC),
                            equalTo(Timestamp.valueOf("2022-06-14 18:54:24.547557")
                                .toLocalDateTime().toInstant(ZoneOffset.UTC))
                        );
                        assertThat(
                            row.getString(columnNames[8]),
                            equalTo(String.valueOf(i))
                        );

                        // same value for all columns
                        assertThat(row.getBoolean(columnNames[9]), equalTo(true));
                    }
                );
            }
        }
    }
}
