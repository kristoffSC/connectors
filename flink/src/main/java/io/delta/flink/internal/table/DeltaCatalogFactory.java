package io.delta.flink.internal.table;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

public class DeltaCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(Context context) {
        // TODO DC - get properties, look for defaultDatabase name etc.
        return new DeltaCatalog(context.getName(), "default");
    }

    @Override
    public String factoryIdentifier() {
        return "delta-catalog";
    }
}
