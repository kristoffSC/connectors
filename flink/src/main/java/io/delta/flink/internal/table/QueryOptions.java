package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.flink.internal.table.DeltaFlinkJobSpecificOptions.QueryMode;

public class QueryOptions {

    private final String deltaTablePath;

    private final QueryMode queryMode;

    private final Map<String, String> jobSpecificOptions = new HashMap<>();

    public QueryOptions(
            String deltaTablePath,
            QueryMode queryMode,
            Map<String, String> jobSpecificOptions) {
        this.deltaTablePath = deltaTablePath;
        this.queryMode = queryMode;
        this.jobSpecificOptions.putAll(jobSpecificOptions);
    }

    public String getDeltaTablePath() {
        return deltaTablePath;
    }

    public QueryMode getQueryMode() {
        return queryMode;
    }

    /**
     * @return an unmodifiable {@code java.util.Map} containing job specific options.
     */
    public Map<String, String> getJobSpecificOptions() {
        return Collections.unmodifiableMap(jobSpecificOptions);
    }

}
