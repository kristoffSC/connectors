package io.delta.flink.internal.table;

import java.io.Serializable;
import java.util.Map;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogFactory;
import org.apache.flink.table.catalog.exceptions.CatalogException;
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
            // We had to add extra dependency to have access to HiveCatalogFactory.
            // "org.apache.flink" % "flink-connector-hive_2.12" % flinkVersion % "provided",
            // "org.apache.flink" % "flink-table-planner_2.12" % flinkVersion % "provided",
            // and remove "org.apache.flink" % "flink-table-test-utils" % flinkVersion % "test",
            // but this causes delta CI to fail for scala 2.11.12.
            // https://github.com/delta-io/connectors/actions/runs/4115104278/jobs/7103364080
            // Maybe we should be able to use flink-table-test-utils for Hive dependencies if
            // this would be resolved after https://issues.apache.org/jira/browse/FLINK-27786.
            // returning null for now, needs further discussion.
            // return new HiveCatalogFactory().createCatalog(context);
            throw new CatalogException("Hive decorated catalog is not yet supported.");
        }
    }

}
