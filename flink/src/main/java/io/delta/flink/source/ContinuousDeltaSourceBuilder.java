package io.delta.flink.source;

import io.delta.flink.source.internal.builder.BaseDeltaSourceBuilder;
import io.delta.flink.source.internal.builder.DeltaBulkFormat;
import io.delta.flink.source.internal.enumerator.ContinuousSplitEnumeratorProvider;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.IGNORE_DELETES;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.UPDATE_CHECK_INTERVAL;

public class ContinuousDeltaSourceBuilder<T>
    extends BaseDeltaSourceBuilder<T, ContinuousDeltaSourceBuilder<T>> {

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode.
     */
    protected static final ContinuousSplitEnumeratorProvider
        DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER =
        new ContinuousSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    ContinuousDeltaSourceBuilder(Path tablePath,
        DeltaBulkFormat<T> bulkFormat, Configuration hadoopConfiguration) {
        super(tablePath, bulkFormat, hadoopConfiguration);
    }

    /**
     * Sets value of "staringVersion" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option specifies the {@link io.delta.standalone.Snapshot} version from which
     * we want to start reading the changes.
     *
     * <p>
     * This option is mutual exclusive with {@link #startingTimestamp(String)} option.
     *
     * @param startingVersion Delta {@link io.delta.standalone.Snapshot} version to start reading
     *                        changes from. The values can be string numbers like "1", "10" etc. or
     *                        keyword "latest", where in that case, changes from the latest Delta
     *                        table version will be read.
     */
    public ContinuousDeltaSourceBuilder<T> startingVersion(String startingVersion) {
        validateOptionValue(STARTING_VERSION.key(), startingVersion);
        sourceConfiguration.addOption(STARTING_VERSION.key(), startingVersion);
        return this;
    }

    /**
     * Sets value of "staringVersion" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option specifies the {@link io.delta.standalone.Snapshot} version from which
     * we want to start reading the changes.
     *
     * <p>
     * This option is mutual exclusive with {@link #startingTimestamp(String)} option.
     *
     * @param startingVersion Delta {@link io.delta.standalone.Snapshot} version to start reading
     *                        changes from.
     */
    public ContinuousDeltaSourceBuilder<T> startingVersion(long startingVersion) {
        startingVersion(String.valueOf(startingVersion));
        return this;
    }

    /**
     * Sets value of "startingTimestamp" option. Applicable for {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode only. This
     * option is used to read only changes from {@link io.delta.standalone.Snapshot} that was
     * generated at or before given timestamp.
     *
     * <p>
     * This option is mutual exclusive with {@link #startingVersion(String)} and {@link
     * #startingVersion(long)} option.
     *
     * @param startingTimestamp The timestamp of {@link io.delta.standalone.Snapshot} that we start
     *                          reading changes from.
     */
    public ContinuousDeltaSourceBuilder<T> startingTimestamp(String startingTimestamp) {
        validateOptionValue(STARTING_TIMESTAMP.key(), startingTimestamp);
        sourceConfiguration.addOption(STARTING_TIMESTAMP.key(), startingTimestamp);
        return this;
    }

    /**
     * Sets the value for "updateCheckIntervalMillis" option. Applicable for {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode only. This
     * option to specify check interval (in milliseconds) used for periodic Delta table changes
     * checks.
     *
     * <p>
     * The default value for this option is 5000 ms.
     *
     * @param updateCheckInterval The update check internal in milliseconds.
     */
    public ContinuousDeltaSourceBuilder<T> updateCheckIntervalMillis(long updateCheckInterval) {
        validateOptionValue(UPDATE_CHECK_INTERVAL.key(), updateCheckInterval);
        sourceConfiguration.addOption(UPDATE_CHECK_INTERVAL.key(), updateCheckInterval);
        return this;
    }

    /**
     * Sets an "ignoreDeletes" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option allows processing Delta table versions containing only {@link
     * io.delta.standalone.actions.RemoveFile} actions.
     *
     * <p> If this option is set to true, Source connector will not throw an exception when
     * processing version containing only {@link io.delta.standalone.actions.RemoveFile} actions
     * regardless of {@link io.delta.standalone.actions.RemoveFile#isDataChange()} flag.
     *
     * <p>
     * The default value for these options is false.
     */
    public ContinuousDeltaSourceBuilder<T> ignoreDeletes(boolean ignoreDeletes) {
        sourceConfiguration.addOption(IGNORE_DELETES.key(), ignoreDeletes);
        return this;
    }

    /**
     * Sets "ignoreChanges" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option allows processing Delta table versions containing both {@link
     * io.delta.standalone.actions.RemoveFile} and {@link io.delta.standalone.actions.AddFile}
     * actions. This option subsumes {@link #ignoreDeletes} option.
     *
     * <p> If this option is set to true, Source connector will not
     * throw an exception when processing version containing combination of {@link
     * io.delta.standalone.actions.RemoveFile} and {@link io.delta.standalone.actions.AddFile}
     * actions regardless of {@link io.delta.standalone.actions.RemoveFile#isDataChange()} flag.
     */
    public ContinuousDeltaSourceBuilder<T> ignoreChanges(boolean ignoreChanges) {
        sourceConfiguration.addOption(IGNORE_DELETES.key(), ignoreChanges);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option {@link String} value to set.
     */
    public ContinuousDeltaSourceBuilder<T> option(String optionName, String optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option boolean value to set.
     */
    public ContinuousDeltaSourceBuilder<T> option(String optionName, boolean optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option int value to set.
     */
    public ContinuousDeltaSourceBuilder<T> option(String optionName, int optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option long value to set.
     */
    public ContinuousDeltaSourceBuilder<T> option(String optionName, long optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return this;
    }


    @SuppressWarnings("unchecked")
    public DeltaSource<T> build() {
        validateMandatoryOptions();
        validateOptionExclusions();

        return new DeltaSource<>(
            tablePath,
            bulkFormat,
            DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER,
            hadoopConfiguration,
            sourceConfiguration
        );
    }

}
