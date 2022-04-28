package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.builder.validation.Validator;
import io.delta.flink.source.internal.enumerator.BoundedSplitEnumeratorProvider;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.internal.DeltaSourceOptions.VERSION_AS_OF;

public abstract class BoundedDeltaSourceBuilder<T, SELF> extends DeltaSourceBuilderBase<T, SELF> {

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode.
     */
    protected static final BoundedSplitEnumeratorProvider
        DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER =
        new BoundedSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    public BoundedDeltaSourceBuilder(Path tablePath,
        FormatBuilder<T> formatBuilder, Configuration hadoopConfiguration) {
        super(tablePath, formatBuilder, hadoopConfiguration);
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
    public SELF versionAsOf(long snapshotVersion) {
        validateOptionValue(VERSION_AS_OF.key(), snapshotVersion);
        sourceConfiguration.addOption(VERSION_AS_OF.key(), snapshotVersion);
        return self();
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
    public SELF timestampAsOf(String snapshotTimestamp) {
        validateOptionValue(TIMESTAMP_AS_OF.key(), snapshotTimestamp);
        sourceConfiguration.addOption(TIMESTAMP_AS_OF.key(), snapshotTimestamp);
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
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    protected Validator validateOptionExclusions() {

        return new Validator()

            // mutually exclusive check for VERSION_AS_OF and TIMESTAMP_AS_OF in Bounded mode.
            .checkArgument(
                !sourceConfiguration.hasOption(VERSION_AS_OF)
                    || !sourceConfiguration.hasOption(TIMESTAMP_AS_OF),
                prepareOptionExclusionMessage(VERSION_AS_OF.key(), TIMESTAMP_AS_OF.key()));
    }
}
