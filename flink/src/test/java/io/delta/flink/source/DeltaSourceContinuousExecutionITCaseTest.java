package io.delta.flink.source;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class DeltaSourceContinuousExecutionITCaseTest extends DeltaSourceITBase {

    private static final int NUMBER_OF_TABLE_UPDATE_BULKS = 5;

    private static final int ROWS_PER_TABLE_UPDATE = 5;

    /**
     * Number of rows in Delta table before inserting a new data into it.
     */
    private static final int INITIAL_DATA_SIZE = 2;

    private final FailoverType failoverType;

    public DeltaSourceContinuousExecutionITCaseTest(FailoverType failoverType) {
        this.failoverType = failoverType;
    }

    @Parameters(name = "{index}: FailoverType = [{0}]")
    public static Collection<Object[]> param() {
        return Arrays.asList(new Object[][]{
            {FailoverType.NONE}, {FailoverType.TASK_MANAGER}, {FailoverType.JOB_MANAGER}
        });
    }

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void shouldReadTableWithNoUpdates() throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource =
            initContinuousSource(nonPartitionedTablePath, SMALL_TABLE_COLUMN_NAMES, COLUMN_TYPES);

        // WHEN
        // Fail TaskManager or JobManager after half of the records or do not fail anything if
        // FailoverType.NONE.
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new ContinuousTestDescriptor(2),
            (FailCheck) readRows -> readRows == SMALL_TABLE_COUNT / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();
        Set<String> actualNames =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(SMALL_TABLE_COUNT));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(SMALL_TABLE_EXPECTED_VALUES));
    }

    @Test
    public void shouldReadLargeDeltaTableWithNoUpdates() throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource =
            initContinuousSource(
                nonPartitionedLargeTablePath,
                new String[]{"col1", "col2", "col3"},
                new LogicalType[]{new BigIntType(), new BigIntType(), new CharType()});

        // WHEN
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new ContinuousTestDescriptor(LARGE_TABLE_RECORD_COUNT),
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();
        Set<Long> actualValues =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getLong(0))
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(LARGE_TABLE_RECORD_COUNT));
        assertThat("Source Produced Different Rows that were in Delta Table", actualValues.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
    }

    @Test
    // This test updates Delta Table 5 times, so it will take some time to finish. About 1 minute.
    public void shouldReadDeltaTableFromSnapshotAndUpdates() throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource =
            initContinuousSource(nonPartitionedTablePath, SMALL_TABLE_COLUMN_NAMES, COLUMN_TYPES);

        ContinuousTestDescriptor testDescriptor = prepareTableUpdates();

        // WHEN
        List<List<RowData>> resultData =
            testContinuousDeltaSource(failoverType, deltaSource, testDescriptor,
                (FailCheck) readRows -> readRows
                    == (INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE)
                    / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();
        Set<String> actualSurNames =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE));
        assertThat("Source Produced Different Rows that were in Delta Table", actualSurNames.size(),
            equalTo(INITIAL_DATA_SIZE + NUMBER_OF_TABLE_UPDATE_BULKS * ROWS_PER_TABLE_UPDATE));
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
            testDescriptor.add(RowType.of(COLUMN_TYPES, SMALL_TABLE_COLUMN_NAMES), newRows);
        }
        return testDescriptor;
    }

    // TODO PR 8 Add tests for Partitions

    // TODO PR 9 for future PRs
    //  This is a temporary method for creating DeltaSourceInternal.
    //  The Desired state is to use DeltaSourceBuilder which was not included in this PR.
    //  For reference how DeltaSourceInternal creation will look like please go to:
    //  https://github.com/delta-io/connectors/pull/256/files#:~:text=testWithoutPartitions()

    private DeltaSource<RowData> initContinuousSource(
        String tablePath, String[] columnNames, LogicalType[] columnTypes) {

        return DeltaSource.forRowDataStepBuilder()
            .tablePath(Path.fromLocalFile(new File(tablePath)))
            .columnNames(columnNames)
            .columnTypes(columnTypes)
            .hadoopConfiguration(DeltaTestUtils.getHadoopConf())
            .continuousMode()
            .build();
    }
}
