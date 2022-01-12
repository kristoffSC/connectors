package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class DeltaSourceBoundedExecutionITCase extends DeltaSourceITBase {

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
    public void testWithoutPartitions() throws Exception {

        // GIVEN
        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            format = buildFormat(
            new String[]{"name", "surname", "age"},
            new LogicalType[]{new CharType(), new CharType(), new IntType()});

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(nonPartitionedTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testBoundDeltaSource(deltaSource);

        Set<String> actualNames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", resultData.size(),
            equalTo(2));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(EXPECTED_NAMES));

        //System.out.println(resultData);
    }

    @Test
    public void testWithOnePartitions() throws Exception {

        // GIVEN
        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            format = buildPartitionedFormat(
            new String[]{"name", "surname", "age", "col2"},
            new LogicalType[]{new CharType(), new CharType(), new IntType(), new CharType()},
            new String[]{"col1", "col2"});

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(partitionedTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testBoundDeltaSource(deltaSource);

        Set<String> actualNames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", resultData.size(),
            equalTo(2));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(EXPECTED_NAMES));

        resultData.forEach(rowData -> {
            assertPartitionValue(rowData, 3, "val2");
        });

        //System.out.println(resultData);
    }

    @Test
    public void testWithBothPartitions() throws Exception {

        // GIVEN
        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            format = buildPartitionedFormat(
            new String[]{"name", "surname", "age", "col1", "col2"},
            new LogicalType[]{new CharType(), new CharType(), new IntType(), new CharType(),
                new CharType()},
            new String[]{"col1", "col2"});

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(partitionedTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testBoundDeltaSource(deltaSource);

        Set<String> actualNames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", resultData.size(),
            equalTo(2));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(EXPECTED_NAMES));

        resultData.forEach(rowData -> {
            assertPartitionValue(rowData, 3, "val1");
            assertPartitionValue(rowData, 4, "val2");
        });

        //System.out.println(resultData);
    }

    @Test
    // Test if SplitEnumerator::addSplitsBack works well,
    // meaning if splits were added back to the Enumerator's state and reassigned to new TM.
    public void testWithTaskManagerFailover() throws Exception {

        // GIVEN
        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            format = buildFormat(
            new String[]{"col1", "col2", "col3"},
            new LogicalType[]{new BigIntType(), new BigIntType(), new CharType()});

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(nonPartitionedLargeTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testBoundDeltaSource(FailoverType.TM, deltaSource,
            (FailCheck) readRows -> readRows == LARGE_TABLE_COUNT / 2);

        Set<Long> actualValues =
            resultData.stream().map(row -> row.getLong(0)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", resultData.size(),
            equalTo(LARGE_TABLE_COUNT));
        assertThat("Source Must Have produced some duplicates.", actualValues.size(),
            equalTo(LARGE_TABLE_COUNT));

        //System.out.println(CountSink.count.get());
        //System.out.println(resultData);
    }

    @Test
    public void testWithJobManagerFailover() throws Exception {
        // TODO Check what we are actually testing with this test. Can't make it fail.
        //  maybe we need bigger test set.

        // GIVEN
        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            format = buildFormat(
            new String[]{"col1", "col2", "col3"},
            new LogicalType[]{new BigIntType(), new BigIntType(), new CharType()});

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(nonPartitionedLargeTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testBoundDeltaSource(FailoverType.JM, deltaSource,
            (FailCheck) readRows -> readRows == LARGE_TABLE_COUNT / 2);

        Set<Long> actualValues =
            resultData.stream().map(row -> row.getLong(0)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", resultData.size(),
            equalTo(LARGE_TABLE_COUNT));
        assertThat("Source Must Have produced some duplicates.", actualValues.size(),
            equalTo(LARGE_TABLE_COUNT));

        //System.out.println(resultData);
    }


    private void assertPartitionValue(RowData rowData, int pos, String val1) {
        assertThat("Partition column has a wrong value.", rowData.getString(pos).toString(),
            equalTo(val1));
    }
}
