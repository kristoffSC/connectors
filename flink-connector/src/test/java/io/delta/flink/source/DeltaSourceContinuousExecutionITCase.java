package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.connector.file.src.ContinuousEnumerationSettings;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DeltaSourceContinuousExecutionITCase extends DeltaSourceITBase {

    public static final LogicalType[] COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};
    public static final String[] COLUMN_NAMES = {"name", "surname", "age"};
    public static final int TABLE_UPDATES_LIM = 5;
    public static final int ROWS_PER_TABLE_UPDATE = 5;
    public static final int INITIAL_DATA_SIZE = 2;
    private static final Set<String> EXPECTED_NAMES =
        Stream.of("Kowalski", "Duda").collect(Collectors.toSet());
    private static final Configuration HADOOP_CONF = DeltaTestUtils.getHadoopConf();
    private static final int LARGE_TABLE_COUNT = 1100;
    private String nonPartitionedTablePath;

    private String nonPartitionedLargeTablePath;

    private String partitionedTablePath;

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
    public void testWithoutUpdates() throws Exception {

        // GIVEN
        DeltaSource<RowData> deltaSource = DeltaSourceBuilder.builder()
            .tablePath(Path.fromLocalFile(new File(nonPartitionedTablePath)))
            .columnNames(COLUMN_NAMES)
            .columnTypes(COLUMN_TYPES)
            .configuration(DeltaTestUtils.getHadoopConf())
            .continuousEnumerationSettings(
                new ContinuousEnumerationSettings(Duration.of(5, ChronoUnit.SECONDS)))
            .build();

        // WHEN
        List<List<RowData>> resultData =
            testContinuousDeltaSource(deltaSource, new ContinuousTestDescriptor(2));

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();
        Set<String> actualNames =
            resultData.stream().flatMap(Collection::stream).map(row -> row.getString(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", totalNumberOfRows,
            equalTo(2));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(EXPECTED_NAMES));
    }

    @Test
    // This test updates Delta Table 5 times, so it will take some time to finish. About 1 minute.
    public void testWithUpdates() throws Exception {

        // TODO make this test parametrized and merge with testWithoutUpdates.
        //  test parameters TABLE_UPDATES_LIM and ROWS_PER_TABLE_UPDATE
        // GIVEN
        DeltaSource<RowData> deltaSource = DeltaSourceBuilder.builder()
            .tablePath(Path.fromLocalFile(new File(nonPartitionedTablePath)))
            .columnNames(COLUMN_NAMES)
            .columnTypes(COLUMN_TYPES)
            .configuration(DeltaTestUtils.getHadoopConf())
            .continuousEnumerationSettings(
                new ContinuousEnumerationSettings(Duration.of(5, ChronoUnit.SECONDS)))
            .build();

        ContinuousTestDescriptor testDescriptor = new ContinuousTestDescriptor(INITIAL_DATA_SIZE);
        for (int i = 0; i < TABLE_UPDATES_LIM; i++) {
            List<Row> newRows = new ArrayList<>();
            for (int j = 0; j < ROWS_PER_TABLE_UPDATE; j++) {
                newRows.add(Row.of("John-" + i + "-" + j, "Wick-" + i + "-" + j, j * i));
            }
            testDescriptor.add(RowType.of(COLUMN_TYPES, COLUMN_NAMES), newRows, newRows.size());
        }

        // WHEN
        List<List<RowData>> resultData =
            testContinuousDeltaSource(deltaSource, testDescriptor);

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

    // TODO Add test for Failvoer TM and JM
    // TODO Add tests for Partitions
    // TODO add tests for Large File

}
