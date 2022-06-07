package io.delta.flink.source.internal.builder;

import java.util.regex.Pattern;

import io.delta.flink.source.internal.DeltaSourceOptions;

/**
 * Implementation of {@link OptionTypeConverter} that validates values for
 * {@link DeltaConfigOption} with type String, where expected value should be a String
 * representation of a positive integer or {@link DeltaSourceOptions#STARTING_VERSION_LATEST}
 * keyword.
 */
public class StartingVersionOptionTypeConverter extends BaseOptionTypeConverter {

    private final Pattern POSITIVE_INT_PATTERN = Pattern.compile("\\d+");

    /**
     * Validates value of {@link DeltaConfigOption} which String value represents positive integers
     * or {@link DeltaSourceOptions#STARTING_VERSION_LATEST} keyword.
     * <p>
     *
     * @param desiredOption  The {@link DeltaConfigOption} instance we want to do the conversion
     *                       for.
     * @param valueToConvert String value to validate.
     * @return A String representing a positive integer or
     * {@link DeltaSourceOptions#STARTING_VERSION_LATEST} keyword.
     * @throws IllegalArgumentException in case of validation failure.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert) {
        Class<T> decoratedType = desiredOption.getValueType();
        OptionType type = OptionType.instanceFrom(decoratedType);

        if (type == OptionType.STRING) {
            if (DeltaSourceOptions.STARTING_VERSION_LATEST.equalsIgnoreCase(valueToConvert)) {
                return (T) valueToConvert;
            }

            if (POSITIVE_INT_PATTERN.matcher(valueToConvert).matches()) {
                return (T) valueToConvert;
            }

            throw new IllegalArgumentException(
                String.format(
                    "Illegal value used for StartingVersionOptionTypeConverter. Expected values "
                        + "are positive integers or \"latest\" keyword (case insensitive). Used "
                        + "value was [%s]", valueToConvert)
                );
        }

        throw new IllegalArgumentException(
            String.format(
                "StartingVersionOptionTypeConverter used with a incompatible DeltaConfigOption "
                    + "option type. This converter must be used only for "
                    + "DeltaConfigOption::String however it was used for '%s' with option '%s'",
                desiredOption.getValueType(), desiredOption.key())
        );
    }
}
