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
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class DeltaSourceBoundedExecutionITCase extends DeltaSourceITBase {

    public static final Set<String> EXPECTED_NAMES =
        Stream.of("Kowalski", "Duda").collect(Collectors.toSet());
    private static final Configuration HADOOP_CONF = DeltaTestUtils.getHadoopConf();

    private String nonPartitionedDeltaTablePath;

    private String partitionedDeltaTablePath;

    @Before
    public void setup() {
        try {
            nonPartitionedDeltaTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            partitionedDeltaTablePath = TMP_FOLDER.newFolder().getAbsolutePath();

            DeltaSinkTestUtils.initTestForNonPartitionedTable(nonPartitionedDeltaTablePath);
            DeltaSinkTestUtils.initTestForPartitionedTable(partitionedDeltaTablePath);
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @Test
    public void testWithoutPartitions() throws Exception {

        // GIVEN
        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            format = buildFormat(
            new String[]{"name", "surname", "age"},
            new LogicalType[]{new CharType(), new CharType(), new IntType()});

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(nonPartitionedDeltaTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testDeltaSource(deltaSource);

        Set<String> actualNames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", resultData.size(),
            equalTo(2));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(EXPECTED_NAMES));

        System.out.println(resultData);
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
            Path.fromLocalFile(new File(partitionedDeltaTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testDeltaSource(deltaSource);

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

        System.out.println(resultData);
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
            Path.fromLocalFile(new File(partitionedDeltaTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testDeltaSource(deltaSource);

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

        System.out.println(resultData);
    }

    @Test
    // Test if SplitEnumerator::addSplitsBack works well,
    // meaning if splits were added back to the Enumerator's state and reassigned to new TM.
    public void testWithTaskManagerFailover() throws Exception {

        // GIVEN
        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            format = buildFormat(
            new String[]{"name", "surname", "age"},
            new LogicalType[]{new CharType(), new CharType(), new IntType()});

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(nonPartitionedDeltaTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testDeltaSource(FailoverType.TM, deltaSource,
            (FailCheck) readRows -> readRows >= 1);

        Set<String> actualNames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", resultData.size(),
            equalTo(2));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(EXPECTED_NAMES));

        System.out.println(resultData);
    }

    @Test
    public void testWithJobManagerFailover() throws Exception {
        // TODO Check what we are actually testing with this test. Can't make it fail.
        //  maybe we need bigger test set.

        // GIVEN
        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            format = buildFormat(
            new String[]{"name", "surname", "age"},
            new LogicalType[]{new CharType(), new CharType(), new IntType()});

        DeltaSource<RowData> deltaSource = DeltaSource.forBulkFileFormat(
            Path.fromLocalFile(new File(nonPartitionedDeltaTablePath)),
            format, new BoundedSplitEnumeratorProvider(), HADOOP_CONF);

        // WHEN
        List<RowData> resultData = testDeltaSource(FailoverType.JM, deltaSource,
            (FailCheck) readRows -> readRows >= 2);

        Set<String> actualNames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.", resultData.size(),
            equalTo(2));
        assertThat("Source Produced Different Rows that were in Delta Table", actualNames,
            equalTo(EXPECTED_NAMES));

        System.out.println(resultData);
    }


    private void assertPartitionValue(RowData rowData, int pos, String val1) {
        assertThat("Partition column has a wrong value.", rowData.getString(pos).toString(),
            equalTo(val1));
    }
}
