package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter;

/**
 * Implementation of {@link OptionTypeConverter} that converts values for
 * {@link DeltaConfigOption} from String Date/Datetime to its timestamp representation in long.
 */
public class TimestampOptionTypeConverter extends BaseOptionTypeConverter {

    /**
     * Converts String value of {@link DeltaConfigOption} that represents Date or Datetime to its
     * timestamp long representation.
     * The implementation uses {@link TimestampFormatConverter} for conversion.
     * See {@link TimestampFormatConverter#convertToTimestamp(String)} for details about
     * allowed formats.
     * @param desiredOption The {@link DeltaConfigOption} instance we want to do the conversion for.
     * @param valueToConvert String representing date or datetime.
     * @return A timestamp representation of valueToConvert returned as long value.
     * @throws IllegalArgumentException in case of conversion failure.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T convertType(DeltaConfigOption<T> desiredOption, String valueToConvert) {
        Class<T> decoratedType = desiredOption.getValueType();
        OptionType type = OptionType.instanceFrom(decoratedType);

        if (type == OptionType.LONG) {
            return (T) (Long) TimestampFormatConverter.convertToTimestamp(valueToConvert);
        }

        throw new IllegalArgumentException(
            String.format(
                "TimestampOptionTypeConverter used with a incompatible DeltaConfigOption "
                    + "option type. This converter must be used only for "
                    + "DeltaConfigOption::Long however it was used for '%s' with option '%s'",
                desiredOption.getValueType(), desiredOption.key())
        );
    }
}
