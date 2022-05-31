package io.delta.flink.source.internal.builder;

import org.apache.flink.configuration.ConfigOption;

public class DeltaConfigOption<T> {

    private final ConfigOption<T> decoratedOption;

    private final Class<T> decoratedType;

    private DeltaConfigOption(ConfigOption<T> decoratedOption, Class<T> type) {
        this.decoratedOption = decoratedOption;
        this.decoratedType = type;
    }

    public static <T> DeltaConfigOption<T> of(ConfigOption<T> configOption, Class<T> type) {
        return new DeltaConfigOption<>(configOption, type);
    }

    public Class<T> getDecoratedType() {
        return decoratedType;
    }

    public String key() {
        return decoratedOption.key();
    }

    public T defaultValue() {
        return decoratedOption.defaultValue();
    }
}
