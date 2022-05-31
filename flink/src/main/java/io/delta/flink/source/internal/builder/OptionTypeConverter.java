package io.delta.flink.source.internal.builder;

/**
 * Utility class to convert {@link DeltaConfigOption} values to desired {@link Class} type.
 */
public final class OptionTypeConverter {

    private OptionTypeConverter() {
    }

    /**
     * Converts an Integer valueToConvert to desired type of
     * {@link DeltaConfigOption#getDecoratedType()}.
     *
     * @param desiredOption  A {@link DeltaConfigOption} to which type the valueToConvert parameter
     *                       should be converted.
     * @param valueToConvert A valueToConvert that type should be converted.
     * @param <T>            A type to which "valueToConvert" parameter will be converted to.
     * @return valueToConvert with converted type to {@link DeltaConfigOption#getDecoratedType()}.
     */
    public static <T> T convertType(DeltaConfigOption<T> desiredOption, Integer valueToConvert) {
        return convertType(desiredOption, String.valueOf(valueToConvert));
    }

    /**
     * Converts a Long valueToConvert to desired type of
     * {@link DeltaConfigOption#getDecoratedType()}.
     *
     * @param desiredOption  A {@link DeltaConfigOption} to which type the valueToConvert parameter
     *                       should be converted.
     * @param valueToConvert A valueToConvert that type should be converted.
     * @param <T>            A type to which "valueToConvert" parameter will be converted to.
     * @return valueToConvert with converted type to {@link DeltaConfigOption#getDecoratedType()}.
     */
    public static <T> T convertType(DeltaConfigOption<T> desiredOption, Long valueToConvert) {
        return convertType(desiredOption, String.valueOf(valueToConvert));
    }

    /**
     * Converts a Boolean valueToConvert to desired type of
     * {@link DeltaConfigOption#getDecoratedType()}.
     *
     * @param desiredOption  A {@link DeltaConfigOption} to which type the valueToConvert parameter
     *                       should be converted.
     * @param valueToConvert A valueToConvert that type should be converted.
     * @param <T>            A type to which "valueToConvert" parameter will be converted to.
     * @return valueToConvert with converted type to {@link DeltaConfigOption#getDecoratedType()}.
     */
    public static <T> T convertType(DeltaConfigOption<T> desiredOption, Boolean valueToConvert) {
        return convertType(desiredOption, String.valueOf(valueToConvert));
    }

    /**
     * Converts a String valueToConvert to desired type of
     * {@link DeltaConfigOption#getDecoratedType()}.
     *
     * @param desiredOption  A {@link DeltaConfigOption} to which type the valueToConvert parameter
     *                       should be converted.
     * @param valueToConvert A valueToConvert that type should be converted.
     * @param <T>            A type to which "valueToConvert" parameter will be converted to.
     * @return valueToConvert with converted type to {@link DeltaConfigOption#getDecoratedType()}.
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert) {
        Class<T> decoratedType = desiredOption.getDecoratedType();
        OptionType type = OptionType.instanceFrom(decoratedType);
        switch (type) {
            case STRING:
                return (T) valueToConvert;
            case BOOLEAN:
                return (T) Boolean.valueOf(valueToConvert);
            case INTEGER:
                return (T) Integer.valueOf(valueToConvert);
            case LONG:
                return (T) Long.valueOf(valueToConvert);
            default:
                throw new RuntimeException("Ehh...");
        }
    }
}
