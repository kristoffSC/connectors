package io.delta.flink.source;

import io.delta.flink.source.internal.builder.BaseDeltaSourceBuilder;
import io.delta.flink.source.internal.builder.DeltaBulkFormat;
import io.delta.flink.source.internal.enumerator.BoundedSplitEnumeratorProvider;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.internal.DeltaSourceOptions.VERSION_AS_OF;

public class BoundedDeltaSourceBuilder<T> extends BaseDeltaSourceBuilder<T> {

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode.
     */
    protected static final BoundedSplitEnumeratorProvider
        DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER =
        new BoundedSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    BoundedDeltaSourceBuilder(Path tablePath,
        DeltaBulkFormat<T> bulkFormat, Configuration hadoopConfiguration) {
        super(tablePath, bulkFormat, hadoopConfiguration);
    }

    /**
     * Sets value of "versionAsOf" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode only. With this option we can time travel to given {@link io.delta.standalone.Snapshot}
     * version and read from it.
     *
     * <p>
     * This option is mutual exclusive with {@link #timestampAsOf(String)} option.
     *
     * @param snapshotVersion Delta {@link io.delta.standalone.Snapshot} version to time travel to.
     */
    public BoundedDeltaSourceBuilder<T> versionAsOf(long snapshotVersion) {
        validateOptionValue(VERSION_AS_OF.key(), snapshotVersion);
        sourceConfiguration.addOption(VERSION_AS_OF.key(), snapshotVersion);
        return this;
    }

    /**
     * Sets value of "timestampAsOf" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode only. With this option we can time travel to the latest {@link
     * io.delta.standalone.Snapshot} that was generated at or before given timestamp.
     * <p>
     * This option is mutual exclusive with {@link #versionAsOf(long)} option.
     *
     * @param snapshotTimestamp The timestamp we should time travel to.
     */
    public BoundedDeltaSourceBuilder<T> timestampAsOf(String snapshotTimestamp) {
        validateOptionValue(TIMESTAMP_AS_OF.key(), snapshotTimestamp);
        sourceConfiguration.addOption(TIMESTAMP_AS_OF.key(), snapshotTimestamp);
        return this;
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option {@link String} value to set.
     */
    public BoundedDeltaSourceBuilder<T> option(String optionName, String optionValue) {
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
    public BoundedDeltaSourceBuilder<T> option(String optionName, boolean optionValue) {
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
    public BoundedDeltaSourceBuilder<T> option(String optionName, int optionValue) {
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
    public BoundedDeltaSourceBuilder<T> option(String optionName, long optionValue) {
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
            DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER,
            hadoopConfiguration,
            sourceConfiguration
        );
    }

}
