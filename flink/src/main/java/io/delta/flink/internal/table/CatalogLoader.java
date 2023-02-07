package io.delta.flink.internal.table;

import java.io.Serializable;
import java.util.Map;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogFactory;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.factories.CatalogFactory.Context;
import org.apache.hadoop.conf.Configuration;

// Iceberg has its own Hive Catalog implementation and his own Catalog "like" interface
// currently we are reusing Flink's classes.
public interface CatalogLoader extends Serializable {

    Catalog createCatalog(Context context);

    static CatalogLoader inMemory() {
        return new InMemoryCatalogLoader();
    }

    static CatalogLoader hive() {
        return new HiveCatalogLoader();
    }

    // TODO DC - not needed for now?
    static CatalogLoader custom(
            String catalogName,
            Map<String, String> properties,
            Configuration hadoopConf, String impl) {
        return null;
    }

    class InMemoryCatalogLoader implements CatalogLoader {

        @Override
        public Catalog createCatalog(Context context) {
            return new GenericInMemoryCatalogFactory().createCatalog(context);
        }
    }

    class HiveCatalogLoader implements CatalogLoader {

        @Override
        public Catalog createCatalog(Context context) {
            // We had to add extra dependency for this
            // "org.apache.flink" % "flink-connector-hive_2.12" % flinkVersion % "provided",
            // "org.apache.flink" % "flink-table-planner_2.12" % flinkVersion % "provided",
            return new HiveCatalogFactory().createCatalog(context);
        }
    }

}
