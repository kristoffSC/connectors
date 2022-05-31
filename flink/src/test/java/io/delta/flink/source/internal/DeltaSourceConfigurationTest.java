package io.delta.flink.source.internal;

import java.util.UUID;

import io.delta.flink.source.internal.builder.DeltaConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.junit.Before;
import org.junit.Test;
import static io.delta.flink.source.internal.DeltaSourceConfigurationTest.TestOptions.BOOLEAN_OPTION;
import static io.delta.flink.source.internal.DeltaSourceConfigurationTest.TestOptions.INT_OPTION;
import static io.delta.flink.source.internal.DeltaSourceConfigurationTest.TestOptions.LONG_OPTION;
import static io.delta.flink.source.internal.DeltaSourceConfigurationTest.TestOptions.NO_DEFAULT_VALUE;
import static io.delta.flink.source.internal.DeltaSourceConfigurationTest.TestOptions.STRING_OPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;

public class DeltaSourceConfigurationTest {

    private DeltaSourceConfiguration configuration;

    @Before
    public void setUp() {
        configuration = new DeltaSourceConfiguration();
    }

    @Test
    public void shouldAddOption() {
        String stringValue = "StringValue";
        long longValue = Long.MIN_VALUE;
        int intValue = Integer.MIN_VALUE;
        boolean booleanValue = true;

        configuration.addOption(LONG_OPTION.key(), longValue);
        configuration.addOption(INT_OPTION.key(), intValue);
        configuration.addOption(STRING_OPTION.key(), stringValue);
        configuration.addOption(BOOLEAN_OPTION.key(), booleanValue);

        assertThat(configuration.hasOption(LONG_OPTION), equalTo(true));
        assertThat(configuration.hasOption(INT_OPTION), equalTo(true));
        assertThat(configuration.hasOption(STRING_OPTION), equalTo(true));
        assertThat(configuration.hasOption(BOOLEAN_OPTION), equalTo(true));

        assertThat(configuration.getValue(LONG_OPTION), equalTo(longValue));
        assertThat(configuration.getValue(INT_OPTION), equalTo(intValue));
        assertThat(configuration.getValue(STRING_OPTION), equalTo(stringValue));
        assertThat(configuration.getValue(BOOLEAN_OPTION), equalTo(booleanValue));
    }

    @Test
    public void shouldGetDefaultValue() {
        assertThat(configuration.hasOption(LONG_OPTION), equalTo(false));
        assertThat(configuration.hasOption(INT_OPTION), equalTo(false));
        assertThat(configuration.hasOption(STRING_OPTION), equalTo(false));
        assertThat(configuration.hasOption(BOOLEAN_OPTION), equalTo(false));

        assertThat(configuration.getValue(LONG_OPTION), equalTo(LONG_OPTION.defaultValue()));
        assertThat(configuration.getValue(INT_OPTION), equalTo(INT_OPTION.defaultValue()));
        assertThat(configuration.getValue(STRING_OPTION), equalTo(STRING_OPTION.defaultValue()));
        assertThat(configuration.getValue(BOOLEAN_OPTION), equalTo(BOOLEAN_OPTION.defaultValue()));
    }

    @Test
    public void shouldHandleNoDefaultValue() {
        assertThat(configuration.hasOption(NO_DEFAULT_VALUE), equalTo(false));
        assertThat(configuration.getValue(NO_DEFAULT_VALUE), nullValue());
    }

    static class TestOptions {

        static final DeltaConfigOption<Long> LONG_OPTION =
            DeltaConfigOption.of(
                ConfigOptions.key("longOption").longType().defaultValue(Long.MAX_VALUE),
                Long.class);

        static final DeltaConfigOption<Integer> INT_OPTION =
            DeltaConfigOption.of(
                ConfigOptions.key("intOption").intType().defaultValue(Integer.MAX_VALUE),
                Integer.class);

        static final DeltaConfigOption<String> STRING_OPTION =
            DeltaConfigOption.of(ConfigOptions.key("stringOption").stringType()
                    .defaultValue(UUID.randomUUID().toString()),
                String.class);

        static final DeltaConfigOption<Boolean> BOOLEAN_OPTION =
            DeltaConfigOption.of(
                ConfigOptions.key("booleanOption").booleanType().defaultValue(false),
                Boolean.class);

        static final DeltaConfigOption<Boolean> NO_DEFAULT_VALUE =
            DeltaConfigOption.of(ConfigOptions.key("noDefault").booleanType().noDefaultValue(),
                Boolean.class);
    }

}
