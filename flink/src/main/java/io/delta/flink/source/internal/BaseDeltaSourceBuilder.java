package io.delta.flink.source.internal;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.IGNORE_DELETES;
import static io.delta.flink.source.internal.DeltaSourceOptions.PARQUET_BATCH_SIZE;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.internal.DeltaSourceOptions.UPDATE_CHECK_INTERVAL;
import static io.delta.flink.source.internal.DeltaSourceOptions.VERSION_AS_OF;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base Builder class for {@link io.delta.flink.source.DeltaSource}
 */
public abstract class BaseDeltaSourceBuilder<T, SELF extends BaseDeltaSourceBuilder<T, SELF>> {

    /**
     * The provider for {@link FileSplitAssigner}.
     */
    protected static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
        LocalityAwareSplitAssigner::new;

    /**
     * The provider for {@link AddFileEnumerator}.
     */
    protected static final AddFileEnumerator.Provider<DeltaSourceSplit>
        DEFAULT_SPLITTABLE_FILE_ENUMERATOR = DeltaFileEnumerator::new;

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode.
     */
    protected static final BoundedSplitEnumeratorProvider
        DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER =
        new BoundedSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode.
     */
    protected static final ContinuousSplitEnumeratorProvider
        DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER =
        new ContinuousSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    /**
     * Message prefix for validation exceptions.
     */
    protected static final String EXCEPTION_PREFIX = "DeltaSourceBuilder - ";

    // -------------- Hardcoded Non Public Options ----------
    /**
     * Hardcoded option for {@link ParquetColumnarRowInputFormat} to threat timestamps as a UTC
     * timestamps.
     */
    protected static final boolean PARQUET_UTC_TIMESTAMP = true;

    /**
     * Hardcoded option for {@link ParquetColumnarRowInputFormat} to use case-sensitive in column
     * name processing for Parquet files.
     */
    protected static final boolean PARQUET_CASE_SENSITIVE = true;
    // ------------------------------------------------------

    /**
     * A placeholder object for Delta source configuration used for {@link BaseDeltaSourceBuilder}
     * instance.
     */
    protected final DeltaSourceConfiguration sourceConfiguration = new DeltaSourceConfiguration();

    /**
     * A {@link Path} to Delta table that should be read by created {@link
     * io.delta.flink.source.DeltaSource}.
     */
    protected final Path tablePath;

    /**
     * A array of column names that should be raed from Delta table by created {@link
     * io.delta.flink.source.DeltaSource}.
     */
    protected final String[] columnNames;

    /**
     * A array of column types ({@link LogicalType} corresponding to {@link
     * BaseDeltaSourceBuilder#columnNames}.
     */
    protected final LogicalType[] columnTypes;

    /**
     * The Hadoop's {@link Configuration} for this Source.
     */
    protected final Configuration hadoopConfiguration;

    /**
     * Flag, that if set to {@code true} indicates that created {@link
     * io.delta.flink.source.DeltaSource} will work in
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}.
     * In other case, created Source will work in
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode.
     */
    protected boolean continuousMode = false;

    /**
     * List of Delta partition columns.
     */
    protected List<String> partitions;

    protected BaseDeltaSourceBuilder(
        Path tablePath, String[] columnNames, LogicalType[] columnTypes,
        Configuration hadoopConfiguration) {
        this.tablePath = tablePath;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    protected static ParquetColumnarRowInputFormat<DeltaSourceSplit> buildFormatWithoutPartitions(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration,
        DeltaSourceConfiguration sourceConfiguration) {

        return new ParquetColumnarRowInputFormat<>(
            configuration,
            RowType.of(columnTypes, columnNames),
            sourceConfiguration.getValue(PARQUET_BATCH_SIZE),
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    /**
     * Sets "versionAsOf"
     */
    public SELF versionAsOf(long snapshotVersion) {
        validateOptionValue(VERSION_AS_OF.key(), snapshotVersion);
        sourceConfiguration.addOption(VERSION_AS_OF.key(), snapshotVersion);
        return self();
    }

    /**
     * Sets "timestampAsOf"
     */
    public SELF timestampAsOf(String snapshotTimestamp) {
        validateOptionValue(TIMESTAMP_AS_OF.key(), snapshotTimestamp);
        sourceConfiguration.addOption(TIMESTAMP_AS_OF.key(), snapshotTimestamp);
        return self();
    }

    /**
     * Sets "startingVersion"
     */
    public SELF startingVersion(String startingVersion) {
        validateOptionValue(STARTING_VERSION.key(), startingVersion);
        sourceConfiguration.addOption(STARTING_VERSION.key(), startingVersion);
        return self();
    }

    /**
     * Sets "startingTimestamp"
     */
    public SELF startingTimestamp(String startingTimestamp) {
        validateOptionValue(STARTING_TIMESTAMP.key(), startingTimestamp);
        sourceConfiguration.addOption(STARTING_TIMESTAMP.key(), startingTimestamp);
        return self();
    }

    /**
     * Sets "updateCheckIntervalMillis"
     */
    public SELF updateCheckIntervalMillis(long updateCheckInterval) {
        validateOptionValue(UPDATE_CHECK_INTERVAL.key(), updateCheckInterval);
        sourceConfiguration.addOption(UPDATE_CHECK_INTERVAL.key(), updateCheckInterval);
        return self();
    }

    /**
     * Sets "ignoreDeletes" flag
     */
    public SELF ignoreDeletes(boolean ignoreDeletes) {
        sourceConfiguration.addOption(IGNORE_DELETES.key(), ignoreDeletes);
        return self();
    }

    /**
     * Sets "ignoreChanges" flag
     */
    public SELF ignoreChanges(boolean ignoreChanges) {
        sourceConfiguration.addOption(IGNORE_DELETES.key(), ignoreChanges);
        return self();
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option {@link String} value to set.
     */
    public SELF option(String optionName, String optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        validateOptionValue(configOption.key(), optionValue);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option boolean value to set.
     */
    public SELF option(String optionName, boolean optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option int value to set.
     */
    public SELF option(String optionName, int optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        validateOptionValue(configOption.key(), optionValue);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option long value to set.
     */
    public SELF option(String optionName, long optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        validateOptionValue(configOption.key(), optionValue);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    /**
     * Set list of partition columns.
     */
    public SELF partitions(List<String> partitions) {
        checkNotNull(partitions, EXCEPTION_PREFIX + "partition list cannot be null.");
        checkArgument(partitions.stream().noneMatch(StringUtils::isNullOrWhitespaceOnly),
            EXCEPTION_PREFIX
                + "List with partition columns contains at least one element that is null, "
                + "empty, or contains only whitespace characters.");

        this.partitions = partitions;
        return self();
    }

    /**
     * Sets source to work in Continuous mode.
     */
    public SELF continuousMode() {
        this.continuousMode = true;
        return self();
    }

    public abstract <V extends DeltaSourceInternal<T>> V build();

    protected void validateMandatoryOptions() {

        // validate against null references
        checkNotNull(tablePath, EXCEPTION_PREFIX + "missing path to Delta table.");
        checkNotNull(columnNames, EXCEPTION_PREFIX + "missing Delta table column names.");
        checkNotNull(columnTypes, EXCEPTION_PREFIX + "missing Delta table column types.");
        checkNotNull(hadoopConfiguration, EXCEPTION_PREFIX + "missing Hadoop configuration.");

        // validate arrays size
        checkArgument(columnNames.length > 0, EXCEPTION_PREFIX + "empty array with column names.");
        checkArgument(columnTypes.length > 0, EXCEPTION_PREFIX + "empty array with column names.");
        checkArgument(columnNames.length == columnTypes.length,
            EXCEPTION_PREFIX + "column names and column types size does not match.");

        // validate invalid array element
        checkArgument(Stream.of(columnNames)
                .noneMatch(StringUtils::isNullOrWhitespaceOnly),
            EXCEPTION_PREFIX + "Column names array contains at least one element that is null, "
                + "empty, or contains only whitespace characters.");
        checkArgument(Stream.of(columnTypes)
            .noneMatch(Objects::isNull), EXCEPTION_PREFIX + "Column type array contains at "
            + "least one null element.");
    }

    protected void validateOptionExclusions() {

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

    protected boolean isContinuousMode() {
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

    private void validateOptionValue(String optionName, String optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }

    private void validateOptionValue(String optionName, int optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }

    private void validateOptionValue(String optionName, long optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }

    @SuppressWarnings("unchecked")
    private SELF self() {
        return (SELF) this;
    }
}
