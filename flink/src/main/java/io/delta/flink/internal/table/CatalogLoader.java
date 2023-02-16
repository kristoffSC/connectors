package io.delta.flink.internal.table;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogFactory;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.factories.CatalogFactory.Context;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.internal.table.DeltaCatalogFactory.CATALOG_TYPE;

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
            Context newContext = filterDeltaCatalogOptions(context);
            return new GenericInMemoryCatalogFactory().createCatalog(newContext);
        }
    }

    class HiveCatalogLoader implements CatalogLoader {

        @Override
        public Catalog createCatalog(Context context) {
            Context newContext = filterDeltaCatalogOptions(context);

            // We had to add extra dependency to have access to HiveCatalogFactory.
            // "org.apache.flink" % "flink-connector-hive_2.12" % flinkVersion % "provided",
            // "org.apache.flink" % "flink-table-planner_2.12" % flinkVersion % "provided",
            // and remove "org.apache.flink" % "flink-table-test-utils" % flinkVersion % "test",
            // but this causes delta CI to fail for scala 2.11.12.
            // https://github.com/delta-io/connectors/actions/runs/4115104278/jobs/7103364080
            // Maybe we should be able to use flink-table-test-utils for Hive dependencies if
            // this would be resolved after https://issues.apache.org/jira/browse/FLINK-27786.
            return new HiveCatalogFactory().createCatalog(newContext);
        }
    }

    default Context filterDeltaCatalogOptions(Context context) {
        Map<String, String> filteredOptions = new HashMap<>(context.getOptions());
        filteredOptions.remove(CATALOG_TYPE);

        return new DeltaCatalogContext(
            context.getName(),
            filteredOptions,
            context.getConfiguration(),
            context.getClassLoader()
        );
    }

}
