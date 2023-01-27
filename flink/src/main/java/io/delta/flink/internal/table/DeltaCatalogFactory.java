package io.delta.flink.internal.table;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.hadoop.conf.Configuration;

public class DeltaCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(Context context) {
        // TODO DC - get properties, look for defaultDatabase name etc.
        // TODO DC - the decorated catalog should be created based on catalog properties.
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
}
