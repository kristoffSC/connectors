package io.delta.flink.source;

import java.util.List;

import io.delta.flink.source.DeltaSourceBuilderSteps.TablePathStep;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceStepBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;

public class RowDataDeltaSourceStepBuilder extends DeltaSourceStepBuilder<RowData> {

    private RowDataDeltaSourceStepBuilder() {
        super();
    }

    /**
     * Creates an instance of Delta Source Step builder with exposed first build step {@link
     * TablePathStep}
     */
    public static TablePathStep<RowData> stepBuilder() {
        return new RowDataDeltaSourceStepBuilder();
    }

    /**
     * Builds an instance of {@link DeltaSource} that will produce elements of {@link RowData} type.
     */
    @Override
    public DeltaSource<RowData> build() {
        // TODO test this
        validateOptionExclusions();

        // TODO add option value validation. Check for null, empty values, numbers for
        //  "string" like values and string for numeric options.
        ParquetColumnarRowInputFormat<DeltaSourceSplit> format = buildFormat();

        return DeltaSource.forBulkFileFormat(tablePath, format,
            (isContinuousMode())
                ? DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER
                : DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER,
            configuration, sourceConfiguration);
    }

    private ParquetColumnarRowInputFormat<DeltaSourceSplit> buildFormat() {
        ParquetColumnarRowInputFormat<DeltaSourceSplit> format;
        if (partitions == null || partitions.isEmpty()) {
            format = buildFormatWithoutPartitions(columnNames, columnTypes, configuration,
                sourceConfiguration);
        } else {
            // TODO PR 8
            throw new UnsupportedOperationException("Partition support will be added later.");
            /*format =
                buildPartitionedFormat(columnNames, columnTypes, configuration, partitions,
                    sourceConfiguration);*/
        }
        return format;
    }

    // TODO After PR 8
    private ParquetColumnarRowInputFormat<DeltaSourceSplit> buildPartitionedFormat(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration,
        List<String> partitionKeys, DeltaSourceConfiguration sourceConfiguration) {

        // TODO After PR 8
        /*return ParquetColumnarRowInputFormat.createPartitionedFormat(
            configuration,
            RowType.of(columnTypes, columnNames),
            partitionKeys, new DeltaPartitionFieldExtractor<>(),
            sourceConfiguration.getValue(PARQUET_BATCH_SIZE),
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);*/
        return null;
    }
}
