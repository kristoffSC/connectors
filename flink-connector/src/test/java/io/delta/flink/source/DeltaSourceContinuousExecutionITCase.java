package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
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
public class DeltaSourceContinuousExecutionITCase extends DeltaSourceITBase {

    public static final LogicalType[] COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};
    public static final String[] COLUMN_NAMES = {"name", "surname", "age"};
    public static final int TABLE_UPDATES_LIM = 5;
    public static final int ROWS_PER_TABLE_UPDATE = 5;
    public static final int INITIAL_DATA_SIZE = 2;
    private static final Set<String> EXPECTED_NAMES =
        Stream.of("Kowalski", "Duda").collect(Collectors.toSet());
    private static final int LARGE_TABLE_COUNT = 1100;
    private static final int SMALL_TABLE_COUNT = 2;
    private final FailoverType failoverType;
    private String nonPartitionedTablePath;
    private String nonPartitionedLargeTablePath;
    private String partitionedTablePath;

    public DeltaSourceContinuousExecutionITCase(
        FailoverType failoverType) {
        this.failoverType = failoverType;
    }

    @Parameters(name = "{index}: FailoverType = [{0}]")
    public static Collection<Object[]> param() {
        return Arrays.asList(new Object[][]{
            {FailoverType.NONE}, {FailoverType.TM}, {FailoverType.JM}
        });
    }

    @Before
    public void setup() {
        try {
            nonPartitionedTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            partitionedTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            nonPartitionedLargeTablePath = TMP_FOLDER.newFolder().getAbsolutePath();

            DeltaSinkTestUtils.initTestForNonPartitionedTable(nonPartitionedTablePath);
            DeltaSinkTestUtils.initTestForPartitionedTable(partitionedTablePath);
            DeltaSinkTestUtils.initTestForNonPartitionedLargeTable(
                nonPartitionedLargeTablePath);
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @After
    public void after() {
        miniClusterResource.getClusterClient().close();
    }

    @Test
    public void testWithNoUpdates() throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = DeltaSourceBuilder.builder()
            .tablePath(Path.fromLocalFile(new File(nonPartitionedTablePath)))
            .columnNames(COLUMN_NAMES)
            .columnTypes(COLUMN_TYPES)
            .hadoopConfiguration(DeltaTestUtils.getHadoopConf())
            .continuousMode(
            )
            .build();

        // WHEN
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new ContinuousTestDescriptor(2),
            (FailCheck) integer -> true);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();
        Set<String> actualNames =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(SMALL_TABLE_COUNT));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(EXPECTED_NAMES));
    }

    @Test
    public void testWithNoUpdatesLargeTable() throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = DeltaSourceBuilder.builder()
            .tablePath(Path.fromLocalFile(new File(nonPartitionedLargeTablePath)))
            .columnNames(new String[]{"col1", "col2", "col3"})
            .columnTypes(new LogicalType[]{new BigIntType(), new BigIntType(), new CharType()})
            .hadoopConfiguration(DeltaTestUtils.getHadoopConf())
            .continuousMode(
            )
            .build();

        // WHEN
        List<List<RowData>> resultData = testContinuousDeltaSource(failoverType, deltaSource,
            new ContinuousTestDescriptor(LARGE_TABLE_COUNT),
            (FailCheck) integer -> true);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();
        Set<Long> actualValues =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getLong(0))
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(LARGE_TABLE_COUNT));
        assertThat("Source Produced Different Rows that were in Delta Table", actualValues.size(),
            equalTo(LARGE_TABLE_COUNT));
    }

    @Test
    // This test updates Delta Table 5 times, so it will take some time to finish. About 1 minute.
    public void testWithUpdates() throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = DeltaSourceBuilder.builder()
            .tablePath(Path.fromLocalFile(new File(nonPartitionedTablePath)))
            .columnNames(COLUMN_NAMES)
            .columnTypes(COLUMN_TYPES)
            .hadoopConfiguration(DeltaTestUtils.getHadoopConf())
            .continuousMode(
            )
            .build();

        ContinuousTestDescriptor testDescriptor = prepareTableUpdates();

        // WHEN
        List<List<RowData>> resultData =
            testContinuousDeltaSource(failoverType, deltaSource, testDescriptor,
                (FailCheck) readRows -> readRows
                    == (INITIAL_DATA_SIZE + TABLE_UPDATES_LIM * ROWS_PER_TABLE_UPDATE) / 2);

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();
        Set<String> actualSurNames =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(INITIAL_DATA_SIZE + TABLE_UPDATES_LIM * ROWS_PER_TABLE_UPDATE));
        assertThat("Source Produced Different Rows that were in Delta Table", actualSurNames.size(),
            equalTo(INITIAL_DATA_SIZE + TABLE_UPDATES_LIM * ROWS_PER_TABLE_UPDATE));
    }

    private ContinuousTestDescriptor prepareTableUpdates() {
        ContinuousTestDescriptor testDescriptor = new ContinuousTestDescriptor(INITIAL_DATA_SIZE);
        for (int i = 0; i < TABLE_UPDATES_LIM; i++) {
            List<Row> newRows = new ArrayList<>();
            for (int j = 0; j < ROWS_PER_TABLE_UPDATE; j++) {
                newRows.add(Row.of("John-" + i + "-" + j, "Wick-" + i + "-" + j, j * i));
            }
            testDescriptor.add(RowType.of(COLUMN_TYPES, COLUMN_NAMES), newRows, newRows.size());
        }
        return testDescriptor;
    }

    // TODO Add tests for Partitions
}
