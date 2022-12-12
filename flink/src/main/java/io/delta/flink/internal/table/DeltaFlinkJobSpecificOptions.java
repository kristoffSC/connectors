package io.delta.flink.internal.table;


import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class DeltaFlinkJobSpecificOptions {

    public static final ConfigOption<String> MODE =
        ConfigOptions.key("mode")
            .stringType()
            .defaultValue("batch");
}
