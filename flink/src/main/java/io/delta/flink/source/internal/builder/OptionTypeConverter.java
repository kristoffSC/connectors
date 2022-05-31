package io.delta.flink.source.internal.builder;


// TODO PR 12 add Javadoc and tests

/**
 * A utility class to convert {@link DeltaConfigOption} values to desired {@link Class} type.
 */
public final class OptionTypeConverter {

    private OptionTypeConverter() {
    }

    /**
     * Converts an integer value to desired type of {@link DeltaConfigOption#getDecoratedType()}.
     *
     * @param option A {@link DeltaConfigOption} for given value.
     * @param value  A value that type should be converted.
     * @param <T>    A type to which "value" parameter will be converted to.
     * @return value with converted type to {@link DeltaConfigOption#getDecoratedType()}.
     */
    public static <T> T convertType(DeltaConfigOption<T> option, Integer value) {
        return convertType(option, String.valueOf(value));
    }

    public static <T> T convertType(DeltaConfigOption<T> option, Long value) {
        return convertType(option, String.valueOf(value));
    }

    public static <T> T convertType(DeltaConfigOption<T> option, Boolean value) {
        return convertType(option, String.valueOf(value));
    }

    @SuppressWarnings("unchecked")
    public static <T> T convertType(DeltaConfigOption<T> option, String value) {
        Class<T> decoratedType = option.getDecoratedType();
        OptionType type = OptionType.instanceFrom(decoratedType);
        switch (type) {
            case STRING:
                return (T) value;
            case BOOLEAN:
                return (T) Boolean.valueOf(value);
            case INTEGER:
                return (T) Integer.valueOf(value);
            case LONG:
                return (T) Long.valueOf(value);
            default:
                throw new RuntimeException("Ehh...");
        }
    }
}
