package io.delta.flink.source.internal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class DeltaSourceOptions implements Serializable {

    public static final ConfigOption<Long> VERSION_AS_OF =
        ConfigOptions.key("versionAsOf").longType().noDefaultValue()
            .withDescription("Snapshot version to read from. Applicable for Bounded Mode only.");

    public static final ConfigOption<Long> TIMESTAMP_AS_OF =
        ConfigOptions.key("timestampAsOf").longType().noDefaultValue()
            .withDescription(
                "Travel back to the latest snapshot that was generated at or before given "
                    + "timestamp. Applicable for Bounded Mode only.");

    public static final ConfigOption<String> STARTING_VERSION =
        ConfigOptions.key("startingVersion").stringType().noDefaultValue()
            .withDescription(
                "Snapshot version to read only changes from. Applicable for Continuous Mode only.");

    public static final ConfigOption<String> STARTING_TIMESTAMP =
        ConfigOptions.key("startingTimestamp").stringType().defaultValue("latest")
            .withDescription(
                "Timestamp to to read only changes from. Applicable for Continuous Mode only.");

    public static final ConfigOption<Integer> UPDATE_CHECK_INTERVAL =
        ConfigOptions.key("updateCheckIntervalMillis").intType().defaultValue(5000)
            .withDescription(
                "Time interval value used for periodical table update checks.");

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

    public static final ConfigOption<Integer> PARQUET_BATCH_SIZE =
        ConfigOptions.key("parquetBatchSize").intType().defaultValue(2048)
            .withDescription("Number of rows read per batch by Parquet Reader from Parquet file.");

    public static final Map<String, ConfigOption<?>> ALLOWED_SOURCE_OPTIONS = new HashMap<>();

    // TODO test all allowed options
    static {
        ALLOWED_SOURCE_OPTIONS.put(VERSION_AS_OF.key(), VERSION_AS_OF);
        ALLOWED_SOURCE_OPTIONS.put(TIMESTAMP_AS_OF.key(), TIMESTAMP_AS_OF);
        ALLOWED_SOURCE_OPTIONS.put(STARTING_VERSION.key(), STARTING_VERSION);
        ALLOWED_SOURCE_OPTIONS.put(STARTING_TIMESTAMP.key(), STARTING_TIMESTAMP);
        ALLOWED_SOURCE_OPTIONS.put(UPDATE_CHECK_INTERVAL.key(), UPDATE_CHECK_INTERVAL);
        ALLOWED_SOURCE_OPTIONS.put(UPDATE_CHECK_INITIAL_DELAY.key(), UPDATE_CHECK_INITIAL_DELAY);
        ALLOWED_SOURCE_OPTIONS.put(IGNORE_DELETES.key(), IGNORE_DELETES);
        ALLOWED_SOURCE_OPTIONS.put(IGNORE_CHANGES.key(), IGNORE_CHANGES);
        ALLOWED_SOURCE_OPTIONS.put(PARQUET_BATCH_SIZE.key(), PARQUET_BATCH_SIZE);
    }

    private final Map<String, Object> usedSourceOptions = new HashMap<>();

    public DeltaSourceOptions addOption(String name, String value) {
        return addOptionObject(name, value);
    }

    public DeltaSourceOptions addOption(String name, boolean value) {
        return addOptionObject(name, value);
    }

    public DeltaSourceOptions addOption(String name, int value) {
        return addOptionObject(name, value);
    }

    public DeltaSourceOptions addOption(String name, long value) {
        return addOptionObject(name, value);
    }

    public boolean hasOption(ConfigOption<?> option) {
        return this.usedSourceOptions.containsKey(option.key());
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue(ConfigOption<T> option) {
        return (T) getValue(option.key()).orElse(option.defaultValue());
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<T> getValue(String optionName) {
        return (Optional<T>) Optional.ofNullable(this.usedSourceOptions.get(optionName));
    }

    private DeltaSourceOptions addOptionObject(String name, Object value) {
        this.usedSourceOptions.put(name, value);
        return this;
    }

}
