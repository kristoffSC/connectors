package io.delta.flink.source.internal.builder;

import java.util.List;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceInternal;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.DeltaFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.internal.DeltaSourceOptions.VERSION_AS_OF;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base Builder class for {@link io.delta.flink.source.DeltaSource}
 */
public abstract class BaseDeltaSourceBuilder<T> {

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
     * Message prefix for validation exceptions.
     */
    protected static final String EXCEPTION_PREFIX = "DeltaSourceBuilder - ";

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

    protected final DeltaBulkFormat<T> bulkFormat;

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
        Path tablePath, DeltaBulkFormat<T> bulkFormat,
        Configuration hadoopConfiguration) {
        this.tablePath = tablePath;
        this.bulkFormat = bulkFormat;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public abstract <V extends DeltaSourceInternal<T>> V build();

    protected void validateMandatoryOptions() {

        // validate against null references
        checkNotNull(tablePath, EXCEPTION_PREFIX + "missing path to Delta table.");
        /*checkNotNull(columnNames, EXCEPTION_PREFIX + "missing Delta table column names.");
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
            + "least one null element.");*/
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

    protected ConfigOption<?> validateOptionName(String optionName) {
        ConfigOption<?> option = DeltaSourceOptions.VALID_SOURCE_OPTIONS.get(optionName);
        if (option == null) {
            throw DeltaSourceExceptions.invalidOptionNameException(
                SourceUtils.pathToString(tablePath), optionName);
        }
        return option;
    }

    protected void validateOptionValue(String optionName, String optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }

    protected void validateOptionValue(String optionName, int optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }

    protected void validateOptionValue(String optionName, long optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }
}
