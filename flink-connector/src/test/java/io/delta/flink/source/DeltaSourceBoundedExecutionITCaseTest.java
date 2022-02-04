package io.delta.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.BoundedSplitEnumeratorProvider;
import io.delta.flink.source.internal.file.DeltaFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class DeltaSourceBoundedExecutionITCaseTest extends DeltaSourceITBase {

    private static final Set<String> EXPECTED_NAMES =
        Stream.of("Kowalski", "Duda").collect(Collectors.toSet());

    private String nonPartitionedTablePath;

    @Before
    public void setup() {
        try {
            nonPartitionedTablePath = TMP_FOLDER.newFolder().getAbsolutePath();

            // TODO Move this from DeltaSinkTestUtils to DeltaTestUtils
            // TODO Add Partitioned table in later PRs
            // TODO Add Large table for JM/TM failovers
            DeltaSinkTestUtils.initTestForNonPartitionedTable(nonPartitionedTablePath);
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
        DeltaSource<RowData> deltaSource =
            initBoundedSource(
                Path.fromLocalFile(new File(nonPartitionedTablePath)),
                new String[]{"name", "surname", "age"},
                new LogicalType[]{new CharType(), new CharType(), new IntType()});

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

    // TODO ADD Partition tests in later PRs
    // TODO ADD Tests for JM and TM failover after adding Split Serializer

    private void assertPartitionValue(RowData rowData, int pos, String val1) {
        assertThat("Partition column has a wrong value.", rowData.getString(pos).toString(),
            equalTo(val1));
    }

    /*
    This is a temporary method for creating DeltaSource.
    The Desired state is to use DeltaSourceBuilder which was not included in this PR.
    For reference how DeltaSource creation will look like please go to:
    https://github.com/delta-io/connectors/pull/256/files#:~:text=testWithoutPartitions()
     */
    private DeltaSource<RowData> initBoundedSource(Path nonPartitionedTablePath,
        String[] columnNames, LogicalType[] columnTypes) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            fileSourceSplitParquetColumnarRowInputFormat = new ParquetColumnarRowInputFormat<>(
            hadoopConf,
            RowType.of(columnTypes, columnNames),
            2048, // Parquet Reader batchSize
            true, // isUtcTimestamp
            true);// isCaseSensitive

        return DeltaSource.forBulkFileFormat(
            nonPartitionedTablePath,
            fileSourceSplitParquetColumnarRowInputFormat,
            new BoundedSplitEnumeratorProvider(
                LocalityAwareSplitAssigner::new, DeltaFileEnumerator::new),
            hadoopConf, new DeltaSourceOptions());
    }
}
