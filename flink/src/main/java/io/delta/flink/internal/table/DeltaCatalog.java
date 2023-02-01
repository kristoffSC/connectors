package io.delta.flink.internal.table;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation.Name;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

public class DeltaCatalog {

    private final String catalogName;

    private final Catalog decoratedCatalog;

    private final Configuration hadoopConfiguration;

    DeltaCatalog(String catalogName, Catalog decoratedCatalog, Configuration hadoopConfiguration) {
        this.catalogName = catalogName;
        this.decoratedCatalog = decoratedCatalog;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public CatalogBaseTable getTable(DeltaCatalogBaseTable catalogTable) {

        CatalogBaseTable metastoreTable = catalogTable.getMetastoreTable();

        String tablePath =
            metastoreTable.getOptions().get(DeltaTableConnectorOptions.TABLE_PATH.key());

        DeltaLog deltaLog = DeltaLog.forTable(this.hadoopConfiguration, tablePath);
        Metadata deltaMetadata = deltaLog.update().getMetadata();
        StructType deltaSchema = deltaMetadata.getSchema();

        // TODO DC - handle case when deltaSchema is null.
        Pair<String[], DataType[]> flinkTypesFromDelta =
            DeltaCatalogTableHelper.resolveFlinkTypesFromDelta(deltaSchema);

        return CatalogTable.of(
            Schema.newBuilder()
                .fromFields(flinkTypesFromDelta.getKey(), flinkTypesFromDelta.getValue())
                .build(), // don't store any schema in metastore.
            metastoreTable.getComment(),
            deltaMetadata.getPartitionColumns(),
            metastoreTable.getOptions()
        );
    }

    public boolean tableExists(DeltaCatalogBaseTable catalogTable) throws CatalogException {

        CatalogBaseTable metastoreTable = catalogTable.getMetastoreTable();
        String deltaTablePath =
            metastoreTable.getOptions().get(DeltaTableConnectorOptions.TABLE_PATH.key());
        return DeltaLog.forTable(hadoopConfiguration, deltaTablePath).tableExists();
    }

    public void createTable(DeltaCatalogBaseTable catalogTable, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        checkNotNull(catalogTable);

        Map<String, String> ddlOptions = catalogTable.getOptions();
        if (!databaseExists(catalogTable.getDatabaseName())) {
            throw new DatabaseNotExistException(
                this.catalogName,
                catalogTable.getDatabaseName()
            );
        }

        String deltaTablePath = ddlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());
        if (StringUtils.isNullOrWhitespaceOnly(deltaTablePath)) {
            throw new CatalogException("Path to Delta table cannot be null or empty.");
        }

        // DDL options validation
        DeltaCatalogTableHelper.validateDdlOptions(ddlOptions);

        // At this point what we should have in ddlOptions are only delta table
        // properties, connector type, table path and user defined options. We don't want to
        // store connector type or table path in _delta_log, so we will filter those
        Map<String, String> deltaDdlOptions =
            DeltaCatalogTableHelper.filterMetastoreDdlOptions(ddlOptions);

        CatalogBaseTable table = catalogTable.getMetastoreTable();
        ObjectPath tableCatalogPath = catalogTable.getTableCatalogPath();

        // Get Partition columns from DDL;
        List<String> ddlPartitionColumns = ((CatalogTable) table).getPartitionKeys();

        // Get Delta schema from Flink DDL.
        StructType ddlDeltaSchema =
            DeltaCatalogTableHelper.resolveDeltaSchemaFromDdl((ResolvedCatalogTable) table);

        DeltaLog deltaLog = DeltaLog.forTable(hadoopConfiguration, deltaTablePath);
        if (deltaLog.tableExists()) {
            // Table exists on filesystem, now we need to check if table exists in Metastore and
            // if so, throw exception.
            if (this.decoratedCatalog.tableExists(tableCatalogPath)) {
                throw new TableAlreadyExistException(this.catalogName, tableCatalogPath);
            }

            // Table was not present in metastore however it is present on Filesystem, we have to
            // verify if schema, partition spec and properties stored in _delta_log match with DDL.
            // TODO DC - handle case when deltaSchema is null.
            Metadata deltaMetadata = deltaLog.update().getMetadata();

            // Validate ddl schema and partition spec matches _delta_log's.
            DeltaCatalogTableHelper.validateDdlSchemaAndPartitionSpecMatchesDelta(
                deltaTablePath,
                tableCatalogPath,
                ddlPartitionColumns,
                ddlDeltaSchema,
                deltaMetadata
            );

            // Add new properties to metadata.
            // Throw if DDL Delta table properties override previously defined properties from
            // _delta_log.
            Map<String, String> deltaLogProperties =
                DeltaCatalogTableHelper.prepareDeltaTableProperties(
                    deltaDdlOptions,
                    tableCatalogPath,
                    deltaMetadata,
                    false // allowOverride = false
                );

            // deltaLogProperties will have same properties than original metadata + new one,
            // defined in DDL. In that case we want to update _delta_log metadata.
            if (deltaLogProperties.size() != deltaMetadata.getConfiguration().size()) {
                Metadata updatedMetadata = deltaMetadata.copyBuilder()
                    .configuration(deltaLogProperties)
                    .build();

                // add properties to _delta_log
                DeltaCatalogTableHelper
                    .commitToDeltaLog(deltaLog, updatedMetadata, Name.SET_TABLE_PROPERTIES);
            }

            // Add table to metastore
            CatalogTable metastoreTable =
                DeltaCatalogTableHelper.prepareMetastoreTable(
                    table,
                    deltaTablePath,
                    ddlPartitionColumns
                );
            this.decoratedCatalog.createTable(tableCatalogPath, metastoreTable, ignoreIfExists);
        } else {
            // Table does not exist on filesystem, we have to create a new _delta_log
            Metadata metadata = Metadata.builder()
                .schema(ddlDeltaSchema)
                .partitionColumns(ddlPartitionColumns)
                .configuration(deltaDdlOptions)
                .build();

            // create _delta_log
            DeltaCatalogTableHelper.commitToDeltaLog(deltaLog, metadata, Name.CREATE_TABLE);

            CatalogTable metastoreTable = DeltaCatalogTableHelper.prepareMetastoreTable(
                table,
                deltaTablePath,
                ddlPartitionColumns
            );

            // add table to metastore
            this.decoratedCatalog.createTable(tableCatalogPath, metastoreTable, ignoreIfExists);
        }
    }

    public void alterTable(DeltaCatalogBaseTable catalogTable) throws CatalogException {

        // Flink's Default SQL dialect support ALTER statements ONLY for changing table name
        // (Catalog::renameTable(...) and for changing/setting table properties. Schema/partition
        // change for Flink default SQL dialect is not supported.
        Map<String, String> alterTableDdlOptions = catalogTable.getOptions();
        String deltaTablePath =
            alterTableDdlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());

        // DDL options validation
        DeltaCatalogTableHelper.validateDdlOptions(alterTableDdlOptions);

        // At this point what we should have in ddlOptions are only delta table
        // properties, connector type, table path and user defined options. We don't want to
        // store connector type or table path in _delta_log, so we will filter those.
        Map<String, String> deltaAlterTableDdlOptions =
            DeltaCatalogTableHelper.filterMetastoreDdlOptions(alterTableDdlOptions);

        DeltaLog deltaLog = DeltaLog.forTable(hadoopConfiguration, deltaTablePath);
        Metadata originalMetaData = deltaLog.update().getMetadata();

        // Add new properties to metadata.
        // Throw if DDL Delta table properties override previously defined properties from
        // _delta_log.
        Map<String, String> deltaLogProperties =
            DeltaCatalogTableHelper.prepareDeltaTableProperties(
                deltaAlterTableDdlOptions,
                catalogTable.getTableCatalogPath(),
                originalMetaData,
                true // allowOverride = true
            );

        Metadata updatedMetadata = originalMetaData.copyBuilder()
            .configuration(deltaLogProperties)
            .build();

        // add properties to _delta_log
        DeltaCatalogTableHelper
            .commitToDeltaLog(deltaLog, updatedMetadata, Name.SET_TABLE_PROPERTIES);
    }

    private boolean databaseExists(String databaseName) {
        return this.decoratedCatalog.databaseExists(databaseName);
    }
}
