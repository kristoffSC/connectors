package io.delta.flink.source;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class DeltaSourceOptions implements Serializable {

    public static final ConfigOption<Long> VERSION_AS_OF =
        ConfigOptions.key("versionAsOf").longType().noDefaultValue()
            .withDescription("Snapshot version to read from.");

    public static final ConfigOption<Long> TIMESTAMP_AS_OF =
        ConfigOptions.key("timestampAsOf").longType().noDefaultValue()
            .withDescription(
                "Travel back to the latest snapshot that was generated at or before given "
                    + "timestamp.");

    public static final ConfigOption<Long> UPDATE_CHECK_INTERVAL =
        ConfigOptions.key("updateCheckIntervalMillis").longType().defaultValue(5000L)
            .withDescription(
                "Time interval value used for periodical table update checks.");

    public static final ConfigOption<Boolean> IGNORE_DELETES =
        ConfigOptions.key("ignoreDeletes").booleanType().defaultValue(false)
            .withDescription("Allow for Delete only versions");

    public static final ConfigOption<Boolean> IGNORE_CHANGES =
        ConfigOptions.key("ignoreChanges").booleanType().defaultValue(false)
            .withDescription("Allow for versions with deletes and updates.");

    public static final ConfigOption<Integer> PARQUET_BATCH_SIZE =
        ConfigOptions.key("parquetBatchSize").intType().defaultValue(500)
            .withDescription("Number of rows read by Parquet Reader from Parquet file per batch.");


    public static final Map<String, ConfigOption<?>> SOURCE_OPTIONS = new HashMap<>();

    static {
        SOURCE_OPTIONS.put(VERSION_AS_OF.key(), VERSION_AS_OF);
        SOURCE_OPTIONS.put(TIMESTAMP_AS_OF.key(), TIMESTAMP_AS_OF);
        SOURCE_OPTIONS.put(UPDATE_CHECK_INTERVAL.key(), UPDATE_CHECK_INTERVAL);
        SOURCE_OPTIONS.put(IGNORE_DELETES.key(), IGNORE_DELETES);
        SOURCE_OPTIONS.put(IGNORE_CHANGES.key(), IGNORE_CHANGES);
        SOURCE_OPTIONS.put(PARQUET_BATCH_SIZE.key(), PARQUET_BATCH_SIZE);
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

    public boolean hasOption(String name) {
        return this.usedSourceOptions.containsKey(name);
    }

    @SuppressWarnings("unchecked")
    public <T> T getOptionValue(ConfigOption<T> option) {
        return (T) getOptionValue(option.key()).orElse(option.defaultValue());
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<T> getOptionValue(String optionName) {
        return (Optional<T>) Optional.ofNullable(this.usedSourceOptions.get(optionName));
    }

    private DeltaSourceOptions addOptionObject(String name, Object value) {
        this.usedSourceOptions.put(name, value);
        return this;
    }

}
