package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class DeltaSourceContinuousExecutionITCaseTest extends DeltaSourceITBase {

    /**
     * Number of updates done on Delta table, where each updated is bounded into one transaction
     */
    private static final int NUMBER_OF_TABLE_UPDATE_BULKS = 5;

    /**
     * Number of rows added per each update of Delta table
     */
    private static final int ROWS_PER_TABLE_UPDATE = 5;

    /**
     * Number of rows in Delta table before inserting a new data into it.
     */
    private static final int INITIAL_DATA_SIZE = 2;

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
    public void shouldReadTableWithNoUpdates(FailoverType failoverType) throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource =
            initContinuousSource(
                nonPartitionedTablePath,
                SMALL_TABLE_COLUMN_NAMES,
                SMALL_TABLE_COLUMN_TYPES
            );

        // WHEN
        // Fail TaskManager or JobManager after half of the records or do not fail anything if
        // FailoverType.NONE.
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new ContinuousTestDescriptor(INITIAL_DATA_SIZE),
            (FailCheck) readRows -> readRows == SMALL_TABLE_COUNT / 2);

        // total number of read rows.
        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were eny duplicates or missing values we will catch them here by comparing
        // size of that Set to expected number of rows.
        Set<String> uniqueValues =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(SMALL_TABLE_COUNT));
        assertThat("Source Produced Different Rows that were in Delta Table", uniqueValues,
            equalTo(SMALL_TABLE_EXPECTED_VALUES));
    }

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
    @EnumSource(FailoverType.class)
    public void shouldReadLargeDeltaTableWithNoUpdates(FailoverType failoverType) throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource =
            initContinuousSource(
                nonPartitionedLargeTablePath,
                LARGE_TABLE_COLUMN_NAMES,
                LARGE_TABLE_COLUMN_TYPES
            );

        // WHEN
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new ContinuousTestDescriptor(LARGE_TABLE_RECORD_COUNT),
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were eny duplicates or missing values we will catch them here by comparing
        // size of that Set to expected number of rows.
        Set<Long> uniqueValues =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getLong(0))
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(LARGE_TABLE_RECORD_COUNT));
        assertThat("Source Produced Different Rows that were in Delta Table", uniqueValues.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
    }

    @ParameterizedTest(name = "{index}: FailoverType = [{0}]")
    @EnumSource(FailoverType.class)
    // This test updates Delta Table 5 times, so it will take some time to finish.
    public void shouldReadDeltaTableFromSnapshotAndUpdates(FailoverType failoverType)
        throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource =
            initContinuousSource(
                nonPartitionedTablePath,
                SMALL_TABLE_COLUMN_NAMES,
                SMALL_TABLE_COLUMN_TYPES
            );

        ContinuousTestDescriptor testDescriptor = prepareTableUpdates();

        // WHEN
        List<List<RowData>> resultData =
            testContinuousDeltaSource(failoverType, deltaSource, testDescriptor,
                (FailCheck) readRows -> readRows
                    == (INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE)
                    / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were eny duplicates or missing values we will catch them here by comparing
        // size of that Set to expected number of rows.
        Set<String> uniqueValues =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE));
        assertThat("Source Produced Different Rows that were in Delta Table", uniqueValues.size(),
            equalTo(INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE));
    }

    @Override
    protected List<RowData> testWithPartitions(DeltaSource<RowData> deltaSource) throws Exception {
        return testContinuousDeltaSource(FailoverType.NONE, deltaSource,
            new ContinuousTestDescriptor(2), (FailCheck) integer -> true).get(0);
    }

    @Override
    protected DeltaSource<RowData> initPartitionedSource(
            String tablePath,
            String[] columnNames,
            LogicalType[] columnTypes,
            List<String> partitionColumns) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        return DeltaSource.forContinuousRowData(
                Path.fromLocalFile(new File(tablePath)),
                columnNames,
                columnTypes,
                hadoopConf
            ).partitionColumns(partitionColumns)
            .build();
    }

    /**
     * Creates a {@link ContinuousTestDescriptor} for tests. The descriptor created by this method
     * describes a scenario where Delta table will be updated {@link #NUMBER_OF_TABLE_UPDATE_BULKS}
     * times, where every update will contain {@link #ROWS_PER_TABLE_UPDATE} new unique rows.
     */
    private ContinuousTestDescriptor prepareTableUpdates() {
        ContinuousTestDescriptor testDescriptor = new ContinuousTestDescriptor(INITIAL_DATA_SIZE);
        for (int i = 0; i < NUMBER_OF_TABLE_UPDATE_BULKS; i++) {
            List<Row> newRows = new ArrayList<>();
            for (int j = 0; j < ROWS_PER_TABLE_UPDATE; j++) {
                newRows.add(Row.of("John-" + i + "-" + j, "Wick-" + i + "-" + j, j * i));
            }
            testDescriptor.add(
                RowType.of(SMALL_TABLE_COLUMN_TYPES, SMALL_TABLE_COLUMN_NAMES), newRows);
        }
        return testDescriptor;
    }

    private DeltaSource<RowData> initContinuousSource(
        String tablePath, String[] columnNames, LogicalType[] columnTypes) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        return DeltaSource.forContinuousRowData(
            Path.fromLocalFile(new File(tablePath)),
            columnNames,
            columnTypes,
            hadoopConf
        ).build();
    }
}
