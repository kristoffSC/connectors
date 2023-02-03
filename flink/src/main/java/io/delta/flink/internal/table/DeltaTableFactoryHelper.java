package io.delta.flink.internal.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;

public final class DeltaTableFactoryHelper {

    private DeltaTableFactoryHelper() {}

    private static final Set<String> OPTIONS_TO_IGNORE = new HashSet<>();

    static {
        OPTIONS_TO_IGNORE.add(FactoryUtil.CONNECTOR.key());
        OPTIONS_TO_IGNORE.add(DeltaTableConnectorOptions.TABLE_PATH.key());
        OPTIONS_TO_IGNORE.add(DeltaFlinkJobSpecificOptions.MODE.key());
    }

    // TODO DC - create tests for this
    public static QueryOptions validateQueryOptions(Configuration options) {

        validateDeltaTablePathOption(options);

        Map<String, String> jobSpecificOptions = new HashMap<>();

        List<String> invalidOptions = new ArrayList<>();
        for (Entry<String, String> entry : options.toMap().entrySet()) {
            String optionName = entry.getKey();

            if (OPTIONS_TO_IGNORE.contains(optionName)) {
                // skip mandatory options
                continue;
            }

            if (DeltaFlinkJobSpecificOptions.JOB_OPTIONS.contains(optionName)) {
                jobSpecificOptions.put(optionName, entry.getValue());
            } else {
                invalidOptions.add(optionName);
            }
        }

        if (!invalidOptions.isEmpty()) {
            String message = String.format(
                "Only Job specific options are allowed in SQL statement.\n"
                    + "Invalid options used:\n%s\n"
                    + "Allowed options:\n%s",
                invalidOptions,
                Arrays.toString(DeltaFlinkJobSpecificOptions.JOB_OPTIONS.toArray(new String[0])));

            throw new ValidationException(message);
        }

        return new QueryOptions(
            options.get(DeltaTableConnectorOptions.TABLE_PATH),
            options.get(DeltaFlinkJobSpecificOptions.MODE),
            jobSpecificOptions
        );
    }

    public static void validateDeltaTablePathOption(Configuration options) {
        if (!options.contains(DeltaTableConnectorOptions.TABLE_PATH)) {
            throw new ValidationException("Missing path to Delta table");
        }
    }

}
