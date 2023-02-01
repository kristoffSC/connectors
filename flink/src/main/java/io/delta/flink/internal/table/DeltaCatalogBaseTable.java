package io.delta.flink.internal.table;

import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.FactoryUtil;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaCatalogBaseTable {

    @Nonnull
    private final ObjectPath tableCatalogPath;

    @Nonnull
    private final CatalogBaseTable metastoreTable;

    private final boolean deltaTable;

    public DeltaCatalogBaseTable(ObjectPath tableCatalogPath, CatalogBaseTable metastoreTable) {
        checkNotNull(tableCatalogPath, "Object path cannot be null for DeltaCatalogBaseTable.");
        checkNotNull(metastoreTable, "Metastore table cannot be null for DeltaCatalogBaseTable.");

        this.tableCatalogPath = tableCatalogPath;
        this.metastoreTable = metastoreTable;

        String connectorType = metastoreTable.getOptions().get(FactoryUtil.CONNECTOR.key());
        this.deltaTable = DeltaDynamicTableFactory.IDENTIFIER.equals(connectorType);
    }

    public ObjectPath getTableCatalogPath() {
        return tableCatalogPath;
    }

    public CatalogBaseTable getMetastoreTable() {
        return metastoreTable;
    }

    public boolean isDeltaTable() {
        return deltaTable;
    }

    public Map<String, String> getOptions() {
        return metastoreTable.getOptions();
    }

    public String getDatabaseName() {
        return tableCatalogPath.getDatabaseName();
    }
}
