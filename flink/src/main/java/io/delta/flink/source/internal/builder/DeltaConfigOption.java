package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import org.apache.flink.configuration.ConfigOption;

/**
 * A wrapper class on Flink's {@link ConfigOption} exposing expected type for given option.
 * The type is used for validation and value conversion for used options.
 *
 * @implNote
 * The wrapped {@link ConfigOption} class hides value type in a way that even if we would extend
 * it, we would nto have access to field type.
 */
public class DeltaConfigOption<T> {

    /**
     * Wrapped {@link ConfigOption}
     */
    private final ConfigOption<T> decoratedOption;

    /**
     * Java class type for decorated option value.
     */
    private final Class<T> decoratedType;

    private DeltaConfigOption(ConfigOption<T> decoratedOption, Class<T> type) {
        this.decoratedOption = decoratedOption;
        this.decoratedType = type;
    }

    public static <T> DeltaConfigOption<T> of(ConfigOption<T> configOption, Class<T> type) {
        return new DeltaConfigOption<>(configOption, type);
    }

    /**
     * @return {@link Class} type for option.
     */
    public Class<T> getValueType() {
        return decoratedType;
    }

    /**
     * @return the configuration key.
     */
    public String key() {
        return decoratedOption.key();
    }

    /**
     * @return the default value, or null, if there is no default value.
     */
    public T defaultValue() {
        return decoratedOption.defaultValue();
    }

    //-------Keeping type safety with implementation of a Visitor pattern -------//
    public void setOnConfig(DeltaSourceConfiguration sourceConfiguration, boolean value) {
        T convertedValue = OptionTypeConverter.convertType(this, value);
        sourceConfiguration.addOption(this, convertedValue);
    }

    public void setOnConfig(DeltaSourceConfiguration sourceConfiguration, int value) {
        T convertedValue = OptionTypeConverter.convertType(this, value);
        sourceConfiguration.addOption(this, convertedValue);
    }

    public void setOnConfig(DeltaSourceConfiguration sourceConfiguration, long value) {
        T convertedValue = OptionTypeConverter.convertType(this, value);
        sourceConfiguration.addOption(this, convertedValue);
    }

    public void setOnConfig(DeltaSourceConfiguration sourceConfiguration, String value) {
        T convertedValue = OptionTypeConverter.convertType(this, value);
        sourceConfiguration.addOption(this, convertedValue);
    }
    //---------------------------------------------------------------------------//
}
