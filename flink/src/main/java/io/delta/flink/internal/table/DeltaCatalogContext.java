package io.delta.flink.internal.table;

import java.util.Map;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.CatalogFactory;

public class DeltaCatalogContext implements CatalogFactory.Context {

    private final String catalogName;

    private final Map<String, String> options;

    private final ReadableConfig configuration;

    private final ClassLoader classLoader;

    public DeltaCatalogContext(
            String catalogName,
            Map<String, String> options,
            ReadableConfig configuration,
            ClassLoader classLoader) {
        this.catalogName = catalogName;
        this.options = options;
        this.configuration = configuration;
        this.classLoader = classLoader;
    }

    @Override
    public String getName() {
        return catalogName;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public ReadableConfig getConfiguration() {
        return configuration;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
