package io.delta.flink.source.internal.enumerator;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class TimestampFormatConverterTest {

    private final String input;

    private final long expected;

    public TimestampFormatConverterTest(String input, long expected) {
        this.input = input;
        this.expected = expected;
    }

    @Parameters(name = "{index}: Input = [{0}]")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {"2022-02-24", 1645660800000L},
            {"2022-02-24 04:55:00", 1645678500000L},
            {"2022-02-24 04:55:00.001", 1645678500001L},
            {"2022-02-24T04:55:00", 1645678500000L},
            {"2022-02-24T04:55:00.001", 1645678500001L},
            {"2022-02-24T04:55:00.001Z", 1645678500001L},
        });
    }

    @Test
    public void shouldConvertToTimestamp() {
        long convert = TimestampFormatConverter.convertToTimestamp(input);
        assertThat(convert, equalTo(expected));
    }
}
