package io.delta.flink.source;

import java.util.List;

import io.delta.flink.source.DeltaSourceBuilderSteps.BuildStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.HadoopConfigurationStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.TableColumnNameStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.TableColumnTypeStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.TablePathStep;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceStepBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;
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
     * {@inheritDoc}
     */
    @Override
    public TableColumnNameStep<RowData> tablePath(Path tablePath) {
        return super.tablePath(tablePath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableColumnTypeStep<RowData> columnNames(String[] columnNames) {
        return super.columnNames(columnNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HadoopConfigurationStep<RowData> columnTypes(LogicalType[] columnTypes) {
        return super.columnTypes(columnTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> hadoopConfiguration(Configuration configuration) {
        return super.hadoopConfiguration(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> versionAsOf(long snapshotVersion) {
        return super.versionAsOf(snapshotVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> timestampAsOf(long snapshotTimestamp) {
        return super.timestampAsOf(snapshotTimestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> startingVersion(long startingVersion) {
        return super.startingVersion(startingVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> startingTimestamp(long startingTimestamp) {
        return super.startingTimestamp(startingTimestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> updateCheckIntervalMillis(long updateCheckInterval) {
        return super.updateCheckIntervalMillis(updateCheckInterval);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> ignoreDeletes(long ignoreDeletes) {
        return super.ignoreDeletes(ignoreDeletes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> ignoreChanges(long ignoreChanges) {
        return super.ignoreChanges(ignoreChanges);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> option(String optionName, String optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> option(String optionName, boolean optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> option(String optionName, int optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> option(String optionName, long optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> partitions(List<String> partitions) {
        return super.partitions(partitions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BuildStep<RowData> continuousMode() {
        return super.continuousMode();
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
