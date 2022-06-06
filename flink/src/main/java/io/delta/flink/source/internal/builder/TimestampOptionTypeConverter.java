package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter;

public class TimestampOptionTypeConverter extends BaseOptionTypeConverter {

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
                    + "DeltaConfigOption::Long but it was used for '%s' with option '%s'",
                desiredOption.getValueType(), desiredOption.key())
        );
    }
}
