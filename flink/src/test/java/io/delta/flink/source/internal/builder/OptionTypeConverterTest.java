package io.delta.flink.source.internal.builder;

import org.junit.jupiter.api.Test;
import static io.delta.flink.source.internal.utils.TestOptions.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OptionTypeConverterTest {

    @Test
    public void shouldConvertToString() {

        String expectedValue = "1";

        assertThat(OptionTypeConverter.convertType(STRING_OPTION, 1), equalTo(expectedValue));
        assertThat(OptionTypeConverter.convertType(STRING_OPTION, 1L), equalTo(expectedValue));
        assertThat(OptionTypeConverter.convertType(STRING_OPTION, true), equalTo("true"));
        assertThat(OptionTypeConverter.convertType(STRING_OPTION, "1"), equalTo(expectedValue));
    }

    @Test
    public void shouldConvertToInteger() {

        int expectedValue = 1;

        assertThat(OptionTypeConverter.convertType(INT_OPTION, 1), equalTo(expectedValue));
        assertThat(OptionTypeConverter.convertType(INT_OPTION, 1L), equalTo(expectedValue));
        assertThat(OptionTypeConverter.convertType(INT_OPTION, "1"), equalTo(expectedValue));

        assertThrows(NumberFormatException.class,
            () -> OptionTypeConverter.convertType(INT_OPTION, true));
    }

    @Test
    public void shouldConvertToLong() {

        long expectedValue = 1L;

        assertThat(OptionTypeConverter.convertType(LONG_OPTION, 1), equalTo(expectedValue));
        assertThat(OptionTypeConverter.convertType(LONG_OPTION, 1L), equalTo(expectedValue));
        assertThat(OptionTypeConverter.convertType(LONG_OPTION, "1"), equalTo(expectedValue));

        assertThrows(NumberFormatException.class,
            () -> OptionTypeConverter.convertType(LONG_OPTION, true));
    }

    @Test
    public void shouldConvertToBoolean() {

        assertThat(OptionTypeConverter.convertType(BOOLEAN_OPTION, 1), equalTo(false));
        assertThat(OptionTypeConverter.convertType(BOOLEAN_OPTION, 1L), equalTo(false));
        assertThat(OptionTypeConverter.convertType(BOOLEAN_OPTION, "1"), equalTo(false));
        assertThat(OptionTypeConverter.convertType(BOOLEAN_OPTION, "0"), equalTo(false));
        assertThat(OptionTypeConverter.convertType(BOOLEAN_OPTION, "true"), equalTo(true));
        assertThat(OptionTypeConverter.convertType(BOOLEAN_OPTION, "false"), equalTo(false));
        assertThat(OptionTypeConverter.convertType(BOOLEAN_OPTION, true), equalTo(true));
        assertThat(OptionTypeConverter.convertType(BOOLEAN_OPTION, false), equalTo(false));
    }
}
