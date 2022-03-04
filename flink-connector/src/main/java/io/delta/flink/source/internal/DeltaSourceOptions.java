package io.delta.flink.source.internal;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * This class contains all available options for {@link io.delta.flink.source.DeltaSource} with
 * their type and default values. It may be viewed as a kind of dictionary class. This class will be
 * used both by Streaming and Table source.
 *
 * @implNote This class is used as a dictionary to work with {@link DeltaSourceConfiguration} class
 * that contains an actual configuration options used for particular {@code DeltaSource} instance.
 */
public class DeltaSourceOptions {

    /**
     * A map of all valid {@code DeltaSource} options. This map can be used for example by {@code
     * DeltaSourceBuilder} to do configuration sanity check.
     *
     * @implNote All {@code ConfigOption} defined in {@code DeltaSourceOptions} class must be added
     * to {@code VALID_SOURCE_OPTIONS} map.
     */
    public static final Map<String, ConfigOption<?>> VALID_SOURCE_OPTIONS = new HashMap<>();

    /**
     * An option that allow time travel to {@link io.delta.standalone.Snapshot} version to read
     * from. Applicable for {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode
     * only.
     * <p>
     * <p>
     * The String representation for this option is <b>versionAsOf</b>.
     */
    public static final ConfigOption<Long> VERSION_AS_OF =
        ConfigOptions.key("versionAsOf").longType().noDefaultValue();

    /**
     * An option that allow time travel to the latest {@link io.delta.standalone.Snapshot} that was
     * generated at or before given timestamp. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode only.
     * <p>
     * <p>
     * The String representation for this option is <b>timestampAsOf</b>.
     */
    public static final ConfigOption<Long> TIMESTAMP_AS_OF =
        ConfigOptions.key("timestampAsOf").longType().noDefaultValue();

    /**
     * An option to specify a {@link io.delta.standalone.Snapshot} version to only read changes
     * from. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only.
     * <p>
     * <p>
     * The String representation for this option is <b>startingVersion</b>.
     */
    public static final ConfigOption<String> STARTING_VERSION =
        ConfigOptions.key("startingVersion").stringType().defaultValue("latest");

    /**
     * An option used to read only changes from {@link io.delta.standalone.Snapshot} that was
     * generated at or before given timestamp. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only.
     * <p>
     * <p>
     * The String representation for this option is <b>startingTimestamp</b>.
     */
    public static final ConfigOption<String> STARTING_TIMESTAMP =
        ConfigOptions.key("startingTimestamp").stringType().noDefaultValue();

    /**
     * An option to specify check interval for monitoring Delta table changes used by {@link
     * io.delta.flink.source.internal.enumerator.ContinuousDeltaSourceSplitEnumerator}. Applicable
     * for {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode
     * only.
     * <p>
     * <p>
     * The String representation for this option is <b>updateCheckIntervalMillis</b>.
     */
    public static final ConfigOption<Integer> UPDATE_CHECK_INTERVAL =
        ConfigOptions.key("updateCheckIntervalMillis").intType().defaultValue(5000);

    public static final ConfigOption<Integer> UPDATE_CHECK_INITIAL_DELAY =
        ConfigOptions.key("updateCheckDelayMillis").intType().defaultValue(1000)
            .withDescription(
                "Time interval value used for periodical table update checks.");

    public static final ConfigOption<Boolean> IGNORE_DELETES =
        ConfigOptions.key("ignoreDeletes").booleanType().defaultValue(false)
            .withDescription("Allow for Delete only versions");

    public static final ConfigOption<Boolean> IGNORE_CHANGES =
        ConfigOptions.key("ignoreChanges").booleanType().defaultValue(false)
            .withDescription("Allow for versions with deletes and updates.");

    // TODO test all allowed options
    static {
        VALID_SOURCE_OPTIONS.put(VERSION_AS_OF.key(), VERSION_AS_OF);
        VALID_SOURCE_OPTIONS.put(TIMESTAMP_AS_OF.key(), TIMESTAMP_AS_OF);
        VALID_SOURCE_OPTIONS.put(STARTING_VERSION.key(), STARTING_VERSION);
        VALID_SOURCE_OPTIONS.put(STARTING_TIMESTAMP.key(), STARTING_TIMESTAMP);
        VALID_SOURCE_OPTIONS.put(UPDATE_CHECK_INTERVAL.key(), UPDATE_CHECK_INTERVAL);
        VALID_SOURCE_OPTIONS.put(UPDATE_CHECK_INITIAL_DELAY.key(), UPDATE_CHECK_INITIAL_DELAY);
        VALID_SOURCE_OPTIONS.put(IGNORE_DELETES.key(), IGNORE_DELETES);
        VALID_SOURCE_OPTIONS.put(IGNORE_CHANGES.key(), IGNORE_CHANGES);
    }

    // TODO Add other options in future PRs
}
