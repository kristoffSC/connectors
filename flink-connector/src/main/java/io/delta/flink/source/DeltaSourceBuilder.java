package io.delta.flink.source;

import java.util.List;

import io.delta.flink.source.DeltaSourceBuilderSteps.BuildStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.HadoopConfigurationStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.MandatorySteps;
import io.delta.flink.source.DeltaSourceBuilderSteps.TableColumnNamesStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.TableColumnTypesStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.TablePathStep;
import io.delta.flink.source.enumerator.BoundedSplitEnumeratorProvider;
import io.delta.flink.source.enumerator.ContinuousSplitEnumeratorProvider;
import io.delta.flink.source.exceptions.DeltaSourceException;
import io.delta.flink.source.exceptions.DeltaSourceExceptionUtils;
import io.delta.flink.source.file.AddFileEnumerator;
import io.delta.flink.source.file.DeltaFileEnumerator;
import io.delta.flink.source.state.DeltaSourceSplit;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.DeltaSourceOptions.PARQUET_BATCH_SIZE;
import static io.delta.flink.source.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.DeltaSourceOptions.STARTING_VERSION;
import static io.delta.flink.source.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.DeltaSourceOptions.VERSION_AS_OF;

public final class DeltaSourceBuilder {

    public static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
        LocalityAwareSplitAssigner::new;

    public static final AddFileEnumerator.Provider<DeltaSourceSplit>
        DEFAULT_SPLITTABLE_FILE_ENUMERATOR = DeltaFileEnumerator::new;

    private static final BoundedSplitEnumeratorProvider DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER =
        new BoundedSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    private static final ContinuousSplitEnumeratorProvider
        DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER =
        new ContinuousSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    private static final boolean PARQUET_UTC_TIMESTAMP = true;

    private static final boolean PARQUET_CASE_SENSITIVE = true;

    private DeltaSourceBuilder() {

    }

    public static TablePathStep builder() {
        return new BuildSteps();
    }

    private static ParquetColumnarRowInputFormat<DeltaSourceSplit> buildFormatWithoutPartitions(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration,
        DeltaSourceOptions sourceOptions) {

        return new ParquetColumnarRowInputFormat<>(
            configuration,
            RowType.of(columnTypes, columnNames),
            sourceOptions.getValue(PARQUET_BATCH_SIZE),
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    private static ParquetColumnarRowInputFormat<DeltaSourceSplit> buildPartitionedFormat(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration,
        List<String> partitionKeys, DeltaSourceOptions sourceOptions) {

        return ParquetColumnarRowInputFormat.createPartitionedFormat(
            configuration,
            RowType.of(columnTypes, columnNames),
            partitionKeys, new DeltaPartitionFieldExtractor<>(),
            sourceOptions.getValue(PARQUET_BATCH_SIZE),
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    private static class BuildSteps implements MandatorySteps, BuildStep {

        private final DeltaSourceOptions sourceOptions = new DeltaSourceOptions();
        private Path tablePath;
        private String[] columnNames;
        private LogicalType[] columnTypes;
        private Configuration configuration;
        private boolean continuousMode = false;
        private List<String> partitions;

        @Override
        public TableColumnNamesStep tablePath(Path tablePath) {
            this.tablePath = tablePath;
            return this;
        }

        @Override
        public TableColumnTypesStep columnNames(String[] columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        @Override
        public HadoopConfigurationStep columnTypes(LogicalType[] columnTypes) {
            this.columnTypes = columnTypes;
            return this;
        }

        @Override
        public BuildStep hadoopConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        @Override
        public BuildStep option(String optionName, String optionValue) {
            ConfigOption<?> configOption = validateOptionName(optionName);
            sourceOptions.addOption(configOption.key(), optionValue);
            return this;
        }

        @Override
        public BuildStep option(String optionName, boolean optionValue) {
            ConfigOption<?> configOption = validateOptionName(optionName);
            sourceOptions.addOption(configOption.key(), optionValue);
            return this;
        }

        @Override
        public BuildStep option(String optionName, int optionValue) {
            ConfigOption<?> configOption = validateOptionName(optionName);
            sourceOptions.addOption(configOption.key(), optionValue);
            return this;
        }

        @Override
        public BuildStep option(String optionName, long optionValue) {
            ConfigOption<?> configOption = validateOptionName(optionName);
            sourceOptions.addOption(configOption.key(), optionValue);
            return this;
        }

        @Override
        public BuildStep partitions(List<String> partitions) {
            this.partitions = partitions;
            return this;
        }

        @Override
        public BuildStep continuousMode() {
            this.continuousMode = true;
            return this;
        }

        @Override
        public DeltaSource<RowData> buildForRowData() {

            // TODO test this
            validateOptionExclusions();

            // TODO add option value validation. Check for null, empty values, numbers for
            //  "string" like values and string for numeric options.

            ParquetColumnarRowInputFormat<DeltaSourceSplit> format = buildFormat();

            return DeltaSource.forBulkFileFormat(tablePath, format,
                (isContinuousMode())
                    ? DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER
                    : DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER,
                configuration, sourceOptions);
        }

        private ParquetColumnarRowInputFormat<DeltaSourceSplit> buildFormat() {
            ParquetColumnarRowInputFormat<DeltaSourceSplit> format;
            if (partitions == null || partitions.isEmpty()) {
                format = buildFormatWithoutPartitions(columnNames, columnTypes, configuration,
                    sourceOptions);
            } else {
                format =
                    buildPartitionedFormat(columnNames, columnTypes, configuration, partitions,
                        sourceOptions);
            }
            return format;
        }

        private void validateOptionExclusions() {

            // mutually exclusive check for VERSION_AS_OF and TIMESTAMP_AS_OF in Bounded mode.
            if (sourceOptions.hasOption(VERSION_AS_OF) && sourceOptions.hasOption(
                TIMESTAMP_AS_OF)) {
                if (!continuousMode) {
                    DeltaSourceExceptionUtils.usedMutualExcludedOptions(VERSION_AS_OF.key(),
                        TIMESTAMP_AS_OF.key());
                }
            }

            // mutually exclusive check for STARTING_VERSION and STARTING_TIMESTAMP in Streaming
            // mode.
            if (sourceOptions.hasOption(STARTING_TIMESTAMP) && sourceOptions.hasOption(
                STARTING_VERSION)) {
                if (continuousMode) {
                    DeltaSourceExceptionUtils.usedMutualExcludedOptions(STARTING_TIMESTAMP.key(),
                        STARTING_VERSION.key());
                }
            }
        }

        private boolean isContinuousMode() {
            return this.continuousMode;
        }

        private ConfigOption<?> validateOptionName(String optionName) {
            ConfigOption<?> option = DeltaSourceOptions.ALLOWED_SOURCE_OPTIONS.get(optionName);
            if (option == null) {
                throw new DeltaSourceException(
                    "Invalid option [" + optionName + "] used for Delta Source Connector.");
            }
            return option;
        }
    }
}
