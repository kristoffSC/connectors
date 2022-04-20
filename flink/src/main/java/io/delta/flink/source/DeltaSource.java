package io.delta.flink.source;

import io.delta.flink.source.builder.DeltaSourceBuilderSteps.TablePathStep;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceInternal;
import io.delta.flink.source.internal.enumerator.SplitEnumeratorProvider;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.actions.AddFile;

/**
 * A unified data source that reads Delta table - both in batch and in streaming mode.
 *
 * <p>This source supports all (distributed) file systems and object stores that can be accessed
 * via the Flink's {@link FileSystem} class.
 *
 * </p>
 *
 * @param <T> The type of the events/records produced by this source.
 * @implNote <h2>Batch and Streaming</h2>
 *
 * <p>This source supports both bounded/batch and continuous/streaming modes. For the
 * bounded/batch case, the Delta Source processes all {@link AddFile} from Delta table Snapshot. In
 * the continuous/streaming case, the source periodically checks the Delta Table for any appending
 * changes and reads them.
 *
 * <h2>Format Types</h2>
 *
 * <p>The reading of each file happens through file readers defined by <i>file format</i>. These
 * define the parsing logic for the contents of the underlying Parquet files.
 *
 * <p>A {@link BulkFormat} reads batches of records from a file at a time.
 * @implNote <h2>Discovering / Enumerating Files</h2>
 * <p>The way that the source lists the files to be processes is defined by the {@code
 * AddFileEnumerator}. The {@code AddFileEnumerator} is responsible to select the relevant {@code
 * AddFile} and to optionally splits files into multiple regions (= file source splits) that can be
 * read in parallel.
 */
// TODO PR 9 include basic bounded + continuous creation example (when BaseDeltaSourceStepBuilder
//  .java API is finalized).
public class DeltaSource<T> extends DeltaSourceInternal<T> {

    DeltaSource(
        Path tablePath, BulkFormat<T, DeltaSourceSplit> readerFormat,
        SplitEnumeratorProvider splitEnumeratorProvider,
        Configuration configuration, DeltaSourceConfiguration sourceConfiguration) {
        super(tablePath, readerFormat, splitEnumeratorProvider, configuration, sourceConfiguration);
    }

    /**
     * Creates a {@link RowDataDeltaSourceStepBuilder} and expose first mandatory build step -
     * {@link TablePathStep}.
     */
    public static TablePathStep<RowData> forRowDataStepBuilder() {
        return RowDataDeltaSourceStepBuilder.stepBuilder();
    }

    /**
     * Creates a {@link RowDataDeltaSourceBuilder}
     *
     * @param tablePath           A {@link Path} to Delta table.
     * @param columnNames         - An array of string with column names that should be read from
     *                            Delta table.
     * @param columnTypes         An array of {@link LogicalType} objects describing a data type for
     *                            each column name defined in {@code columnNames} parameter.
     * @param hadoopConfiguration A Hadoop configuration object.
     */
    public static RowDataDeltaSourceBuilder forRowData(
        Path tablePath, String[] columnNames, LogicalType[] columnTypes,
        Configuration hadoopConfiguration) {
        return RowDataDeltaSourceBuilder.builder(
            tablePath, columnNames, columnTypes, hadoopConfiguration);
    }
}
