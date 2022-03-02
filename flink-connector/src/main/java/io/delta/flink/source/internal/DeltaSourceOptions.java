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

    // TODO test all allowed options
    static {
        VALID_SOURCE_OPTIONS.put(VERSION_AS_OF.key(), VERSION_AS_OF);
        VALID_SOURCE_OPTIONS.put(TIMESTAMP_AS_OF.key(), TIMESTAMP_AS_OF);
    }

    // TODO Add other options in future PRs
}
