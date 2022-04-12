package io.delta.flink.source;

import java.util.List;

import io.delta.flink.source.internal.DeltaSourceBuilderSteps.BuildStep;
import io.delta.flink.source.internal.DeltaSourceBuilderSteps.HadoopConfigurationStep;
import io.delta.flink.source.internal.DeltaSourceBuilderSteps.MandatorySteps;
import io.delta.flink.source.internal.DeltaSourceBuilderSteps.TableColumnNamesStep;
import io.delta.flink.source.internal.DeltaSourceBuilderSteps.TableColumnTypesStep;
import io.delta.flink.source.internal.DeltaSourceBuilderSteps.TablePathStep;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.BoundedSplitEnumeratorProvider;
import io.delta.flink.source.internal.enumerator.ContinuousSplitEnumeratorProvider;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.DeltaFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.IGNORE_DELETES;
import static io.delta.flink.source.internal.DeltaSourceOptions.PARQUET_BATCH_SIZE;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.internal.DeltaSourceOptions.UPDATE_CHECK_INTERVAL;
import static io.delta.flink.source.internal.DeltaSourceOptions.VERSION_AS_OF;

/**
 * The builder for {@link DeltaSource} that follows Build Step Pattern.
 */
public final class DeltaSourceStepBuilder implements MandatorySteps, BuildStep {

    /**
     * The provider for {@link FileSplitAssigner}.
     */
    private static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
        LocalityAwareSplitAssigner::new;

    /**
     * The provider for {@link AddFileEnumerator}.
     */
    private static final AddFileEnumerator.Provider<DeltaSourceSplit>
        DEFAULT_SPLITTABLE_FILE_ENUMERATOR = DeltaFileEnumerator::new;

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode.
     */
    private static final BoundedSplitEnumeratorProvider DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER =
        new BoundedSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode.
     */
    private static final ContinuousSplitEnumeratorProvider
        DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER =
        new ContinuousSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    // -------------- Hardcoded Non Public Options ----------
    /**
     * Hardcoded option for {@link ParquetColumnarRowInputFormat} to threat timestamps as a UTC
     * timestamps.
     */
    private static final boolean PARQUET_UTC_TIMESTAMP = true;

    /**
     * Hardcoded option for {@link ParquetColumnarRowInputFormat} to use case-sensitive in column
     * name processing for Parquet files.
     */
    private static final boolean PARQUET_CASE_SENSITIVE = true;
    // ------------------------------------------------------

    /**
     * A placeholder object for Delta source configuration used for {@link DeltaSourceStepBuilder}
     * instance.
     */
    private final DeltaSourceConfiguration sourceConfiguration = new DeltaSourceConfiguration();

    /**
     * A {@link Path} to Delta table that should be read by created {@link DeltaSource}.
     */
    private Path tablePath;

    /**
     * A array of column names that should be raed from Delta table by created
     * {@link DeltaSource}.
     */
    private String[] columnNames;

    /**
     * A array of column types ({@link LogicalType} corresponding to {@link
     * DeltaSourceStepBuilder#columnNames}.
     */
    private LogicalType[] columnTypes;

    /**
     * The Hadoop's {@link Configuration} for this Source.
     */
    private Configuration configuration;

    /**
     * Flag, that if set to {@code true} indicates that created {@link DeltaSource} will work in
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}. In other
     * case, created Source will work in
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode.
     */
    private boolean continuousMode = false;

    /**
     * List of Delta partition columns.
     */
    private List<String> partitions;

    private DeltaSourceStepBuilder() {

    }

    /**
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    public static TablePathStep builder() {
        return new DeltaSourceStepBuilder();
    }

    private static ParquetColumnarRowInputFormat<DeltaSourceSplit> buildFormatWithoutPartitions(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration,
        DeltaSourceConfiguration sourceConfiguration) {

        return new ParquetColumnarRowInputFormat<>(
            configuration,
            RowType.of(columnTypes, columnNames),
            sourceConfiguration.getValue(PARQUET_BATCH_SIZE),
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    // TODO After PR 8
    private static ParquetColumnarRowInputFormat<DeltaSourceSplit> buildPartitionedFormat(
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

    /**
     * Sets {@link Path} to Delta table.
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public TableColumnNamesStep tablePath(Path tablePath) {
        this.tablePath = tablePath;
        return this;
    }

    /**
     * Defines column names that should be read from Delta table.
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public TableColumnTypesStep columnNames(String[] columnNames) {
        this.columnNames = columnNames;
        return this;
    }

    /**
     * Defines types for column names defined by
     * {@link DeltaSourceStepBuilder#columnNames(String[])}
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public HadoopConfigurationStep columnTypes(LogicalType[] columnTypes) {
        this.columnTypes = columnTypes;
        return this;
    }

    /**
     * Defines Hadoop configuration that should be used by craeted {@link DeltaSource}.
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep hadoopConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    /**
     * Sets "versionAsOf"
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep versionAsOf(long snapshotVersion) {
        sourceConfiguration.addOption(VERSION_AS_OF.key(), snapshotVersion);
        return this;
    }

    /**
     * Sets "timestampAsOf"
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep timestampAsOf(long snapshotTimestamp) {
        sourceConfiguration.addOption(TIMESTAMP_AS_OF.key(), snapshotTimestamp);
        return this;
    }

    /**
     * Sets "startingVersion"
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep startingVersion(long startingVersion) {
        sourceConfiguration.addOption(STARTING_VERSION.key(), startingVersion);
        return this;
    }

    /**
     * Sets "startingTimestamp"
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep startingTimestamp(long startingTimestamp) {
        sourceConfiguration.addOption(STARTING_TIMESTAMP.key(), startingTimestamp);
        return this;
    }

    /**
     * Sets "updateCheckIntervalMillis"
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep updateCheckIntervalMillis(long updateCheckInterval) {
        sourceConfiguration.addOption(UPDATE_CHECK_INTERVAL.key(), updateCheckInterval);
        return this;
    }

    /**
     * Sets "ignoreDeletes" flag
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep ignoreDeletes(long ignoreDeletes) {
        sourceConfiguration.addOption(IGNORE_DELETES.key(), ignoreDeletes);
        return this;
    }

    /**
     * Sets "ignoreChanges" flag
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep ignoreChanges(long ignoreChanges) {
        sourceConfiguration.addOption(IGNORE_DELETES.key(), ignoreChanges);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option {@link String} value to set.
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep option(String optionName, String optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option boolean value to set.
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep option(String optionName, boolean optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option int value to set.
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep option(String optionName, int optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option long value to set.
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep option(String optionName, long optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return this;
    }

    /**
     * Set list of partition columns.
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep partitions(List<String> partitions) {
        this.partitions = partitions;
        return this;
    }

    /**
     * Sets source to work in Continuous mode.
     *
     * @return instance of {@link DeltaSourceStepBuilder}.
     */
    @Override
    public BuildStep continuousMode() {
        this.continuousMode = true;
        return this;
    }

    /**
     * Builds a {@link DeltaSource} instance for
     * {@link org.apache.flink.connector.file.src.reader.BulkFormat}
     * and with {@link RowData} as a type of produced records.
     *
     * @return A new instnace of {@link DeltaSource}.
     */
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

    private void validateOptionExclusions() {

        // mutually exclusive check for VERSION_AS_OF and TIMESTAMP_AS_OF in Bounded mode.
        if (sourceConfiguration.hasOption(VERSION_AS_OF)
            && sourceConfiguration.hasOption(TIMESTAMP_AS_OF)) {
            if (!continuousMode) {
                throw DeltaSourceExceptions.usedMutualExcludedOptionsException(
                    SourceUtils.pathToString(tablePath),
                    VERSION_AS_OF.key(),
                    TIMESTAMP_AS_OF.key());
            }
        }

        // mutually exclusive check for STARTING_VERSION and STARTING_TIMESTAMP in Streaming
        // mode.
        if (sourceConfiguration.hasOption(STARTING_TIMESTAMP) && sourceConfiguration.hasOption(
            STARTING_VERSION)) {
            if (continuousMode) {
                throw DeltaSourceExceptions.usedMutualExcludedOptionsException(
                    SourceUtils.pathToString(tablePath),
                    STARTING_TIMESTAMP.key(),
                    STARTING_VERSION.key());
            }
        }
    }

    private boolean isContinuousMode() {
        return this.continuousMode;
    }

    private ConfigOption<?> validateOptionName(String optionName) {
        ConfigOption<?> option = DeltaSourceOptions.VALID_SOURCE_OPTIONS.get(optionName);
        if (option == null) {
            throw DeltaSourceExceptions.invalidOptionNameException(
                SourceUtils.pathToString(tablePath), optionName);
        }
        return option;
    }
}
