package io.delta.flink.source.internal.builder;

import org.apache.flink.util.StringUtils;

/**
 * Implementation of {@link OptionTypeConverter} that validates values for
 * {@link DeltaConfigOption} with type String, where expected value should be a String.
 */
public class ColumnNamesOptionTypeConverter implements OptionTypeConverter<String> {

    private static final String ERROR_MSG = "%s values are not supported for 'columnNames' option,"
        + " only String values and String collections are supported.";

    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, Integer valueToConvert) {
        throw new IllegalArgumentException(String.format(ERROR_MSG, "Integer"));
    }

    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, Long valueToConvert) {
        throw new IllegalArgumentException(String.format(ERROR_MSG, "Long"));
    }

    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, Boolean valueToConvert) {
        throw new IllegalArgumentException(String.format(ERROR_MSG, "Boolean"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert) {

        if (StringUtils.isNullOrWhitespaceOnly(valueToConvert)) {
            throw new IllegalArgumentException(String.format(ERROR_MSG, "Empty/null String"));
        }

        return (T) valueToConvert;
    }
}
