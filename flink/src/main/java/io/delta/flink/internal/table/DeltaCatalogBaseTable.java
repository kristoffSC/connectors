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
    private final CatalogBaseTable catalogTable;

    private final boolean isDeltaTable;

    public DeltaCatalogBaseTable(ObjectPath tableCatalogPath, CatalogBaseTable catalogTable) {
        checkNotNull(tableCatalogPath, "Object path cannot be null for DeltaCatalogBaseTable.");
        checkNotNull(catalogTable, "Catalog table cannot be null for DeltaCatalogBaseTable.");

        this.tableCatalogPath = tableCatalogPath;
        this.catalogTable = catalogTable;

        String connectorType = catalogTable.getOptions().get(FactoryUtil.CONNECTOR.key());
        this.isDeltaTable =
            DeltaDynamicTableFactory.DELTA_CONNECTOR_IDENTIFIER.equals(connectorType);
    }

    public ObjectPath getTableCatalogPath() {
        return tableCatalogPath;
    }

    public CatalogBaseTable getCatalogTable() {
        return catalogTable;
    }

    public boolean isDeltaTable() {
        return isDeltaTable;
    }

    public Map<String, String> getOptions() {
        return catalogTable.getOptions();
    }

    public String getDatabaseName() {
        return tableCatalogPath.getDatabaseName();
    }
}
