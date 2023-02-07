package io.delta.flink.internal.table;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.hadoop.conf.Configuration;

public class DeltaCatalogFactory implements CatalogFactory {

    public static final String CATALOG_TYPE = "catalog-type";

    public static final String CATALOG_TYPE_HIVE = "hive";

    public static final String CATALOG_TYPE_IN_MEMORY = "in-memory";

    public static final ConfigOption<String> DEFAULT_DATABASE =
        ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
            .stringType()
            .defaultValue("default");

    @Override
    public Catalog createCatalog(Context context) {

        Map<String, String> originalOptions = context.getOptions();
        Map<String, String> deltaContextOptions = new HashMap<>(originalOptions);

        // Making sure that decorated catalog will use the same name for default database.
        if (!deltaContextOptions.containsKey(CommonCatalogOptions.DEFAULT_DATABASE_KEY)) {
            deltaContextOptions.put(DEFAULT_DATABASE.key(), DEFAULT_DATABASE.defaultValue());
        }

        DeltaCatalogContext deltaCatalogContext = new DeltaCatalogContext(
            context.getName(),
            deltaContextOptions,
            context.getConfiguration(),
            context.getClassLoader()
        );

        //Catalog decoratedCatalog = createDecoratedCatalog(deltaCatalogContext);
        Catalog decoratedCatalog = new GenericInMemoryCatalog(context.getName(), "default");
        Configuration hadoopConfiguration =
            HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
        return new CatalogProxy(context.getName(), "default", decoratedCatalog,
            hadoopConfiguration);
    }

    @Override
    public String factoryIdentifier() {
        return "delta-catalog";
    }


    private Catalog createDecoratedCatalog(Context context) {

        Map<String, String> options = context.getOptions();
        String catalogType = options.getOrDefault(CATALOG_TYPE, CATALOG_TYPE_IN_MEMORY);

        switch (catalogType.toLowerCase(Locale.ENGLISH)) {
            case CATALOG_TYPE_HIVE:
                return CatalogLoader.hive().createCatalog(context);
            case CATALOG_TYPE_IN_MEMORY:
                return CatalogLoader.inMemory().createCatalog(context);
            default:
                throw new CatalogException("Unknown catalog-type: " + catalogType +
                    " (Must be 'hive' or 'inMemory')");
        }

    }
}
