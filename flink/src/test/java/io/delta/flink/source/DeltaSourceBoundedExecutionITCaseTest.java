package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

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
        List<String> partitions) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        return DeltaSource.forBoundedRowData(
                Path.fromLocalFile(new File(tablePath)),
                columnNames,
                columnTypes,
                hadoopConf
            ).partitions(partitions)
            .build();
    }
}
