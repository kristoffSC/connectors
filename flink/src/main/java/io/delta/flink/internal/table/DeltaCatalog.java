package io.delta.flink.internal.table;

import java.util.List;
import java.util.Map;

import io.delta.flink.internal.table.DeltaCatalogTableHelper.DeltaMetastoreTable;
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
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

public class DeltaCatalog {

    private final String catalogName;

    /**
     * A Flink's {@link Catalog} implementation to which all Metastore related actions will be
     * redirected. The {@link DeltaCatalog} will not call {@link Catalog#open()} on this instance.
     * If it is required to call this method it should be done before passing this reference to
     * {@link DeltaCatalog}.
     */
    private final Catalog decoratedCatalog;

    private final Configuration hadoopConf;

    /**
     * Creates instance of {@link DeltaCatalog} for given decorated catalog and catalog name.
     *
     * @param catalogName         catalog name.
     * @param decoratedCatalog    A Flink's {@link Catalog} implementation to which all Metastore
     *                            related actions will be redirected. The {@link DeltaCatalog} will
     *                            not call {@link Catalog#open()} on this instance. If it is
     *                            required to call this method it should be done before passing this
     *                            reference to {@link DeltaCatalog}.
     * @param hadoopConf The {@link Configuration} object that will be used for {@link
     *                            DeltaLog} initialization.
     */
    DeltaCatalog(String catalogName, Catalog decoratedCatalog, Configuration hadoopConf) {
        this.catalogName = catalogName;
        this.decoratedCatalog = decoratedCatalog;
        this.hadoopConf = hadoopConf;

        checkArgument(
            !StringUtils.isNullOrWhitespaceOnly(catalogName),
            "Catalog name cannot be null or empty."
        );
        checkArgument(decoratedCatalog != null,
            "The decoratedCatalog cannot be null."
        );
        checkArgument(hadoopConf != null,
            "The Hadoop Configuration object - 'hadoopConfiguration' cannot be null."
        );
    }

    public CatalogBaseTable getTable(DeltaCatalogBaseTable catalogTable)
            throws TableNotExistException {

        CatalogBaseTable metastoreTable = catalogTable.getCatalogTable();

        String tablePath =
            metastoreTable.getOptions().get(DeltaTableConnectorOptions.TABLE_PATH.key());

        DeltaLog deltaLog = DeltaLog.forTable(this.hadoopConf, tablePath);
        if (!deltaLog.tableExists()) {
            // TableNotExistException does not accept custom message, but we would like to meet
            // API contracts from Flink's Catalog::getTable interface and throw
            // TableNotExistException but with information that what was missing was _delta_log.
            throw new TableNotExistException(
                this.catalogName,
                catalogTable.getTableCatalogPath(),
                new CatalogException(
                    String.format(
                        "Table %s exists in metastore but _delta_log was not found under path %s",
                        catalogTable.getTableCatalogPath().getFullName(),
                        tablePath
                    )
                )
            );
        }
        Metadata deltaMetadata = deltaLog.update().getMetadata();
        StructType deltaSchema = deltaMetadata.getSchema();
        if (deltaSchema == null) {
            // This should not happen, but if it did for some reason it mens there is something
            // wong with _delta_log.
            throw new CatalogException(String.format(""
                    + "Delta schema is null for table %s and table path %s. Please contact your "
                    + "administrator.",
                catalogTable.getCatalogTable(),
                tablePath
            ));
        }

        Pair<String[], DataType[]> flinkTypesFromDelta =
            DeltaCatalogTableHelper.resolveFlinkTypesFromDelta(deltaSchema);

        return CatalogTable.of(
            Schema.newBuilder()
                .fromFields(flinkTypesFromDelta.getKey(), flinkTypesFromDelta.getValue())
                .build(), // Table Schema is not stored in metastore, we take it from _delta_log.
            metastoreTable.getComment(),
            deltaMetadata.getPartitionColumns(),
            metastoreTable.getOptions()
        );
    }

    public boolean tableExists(DeltaCatalogBaseTable catalogTable) {

        CatalogBaseTable metastoreTable = catalogTable.getCatalogTable();
        String deltaTablePath =
            metastoreTable.getOptions().get(DeltaTableConnectorOptions.TABLE_PATH.key());
        return DeltaLog.forTable(hadoopConf, deltaTablePath).tableExists();
    }

    public void createTable(DeltaCatalogBaseTable catalogTable, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        checkNotNull(catalogTable);
        ObjectPath tableCatalogPath = catalogTable.getTableCatalogPath();
        // First we need to check if table exists in Metastore and if so, throw exception.
        if (this.decoratedCatalog.tableExists(tableCatalogPath) && !ignoreIfExists) {
            throw new TableAlreadyExistException(this.catalogName, tableCatalogPath);
        }

        boolean databaseExists =
            this.decoratedCatalog.databaseExists(catalogTable.getDatabaseName());
        if (!databaseExists) {
            throw new DatabaseNotExistException(
                this.catalogName,
                catalogTable.getDatabaseName()
            );
        }

        Map<String, String> ddlOptions = catalogTable.getOptions();
        String deltaTablePath = ddlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());
        if (StringUtils.isNullOrWhitespaceOnly(deltaTablePath)) {
            throw new CatalogException("Path to Delta table cannot be null or empty.");
        }

        // DDL options validation
        DeltaCatalogTableHelper.validateDdlOptions(ddlOptions);

        // At this point what we should have in ddlOptions are only delta table
        // properties, connector type, table path and user defined options. We don't want to
        // store connector type or table path in _delta_log, so we will filter those
        Map<String, String> filteredDdlOptions =
            DeltaCatalogTableHelper.filterMetastoreDdlOptions(ddlOptions);

        CatalogBaseTable table = catalogTable.getCatalogTable();

        // Get Partition columns from DDL;
        List<String> ddlPartitionColumns = ((CatalogTable) table).getPartitionKeys();

        // Get Delta schema from Flink DDL.
        StructType ddlDeltaSchema =
            DeltaCatalogTableHelper.resolveDeltaSchemaFromDdl((ResolvedCatalogTable) table);

        // We need to check if table exists in Metastore and if so, throw exception.
        if (this.decoratedCatalog.tableExists(tableCatalogPath) && !ignoreIfExists) {
            throw new TableAlreadyExistException(this.catalogName, tableCatalogPath);
        }

        DeltaLog deltaLog = DeltaLog.forTable(hadoopConf, deltaTablePath);
        if (deltaLog.tableExists()) {
            // Table was not present in metastore however it is present on Filesystem, we have to
            // verify if schema, partition spec and properties stored in _delta_log match with DDL.
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
                    filteredDdlOptions,
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
                DeltaCatalogTableHelper.commitToDeltaLog(
                    deltaLog,
                    updatedMetadata,
                    Operation.Name.SET_TABLE_PROPERTIES
                );
            }

            // Add table to metastore
            DeltaMetastoreTable metastoreTable =
                DeltaCatalogTableHelper.prepareMetastoreTable(table, deltaTablePath);
            this.decoratedCatalog.createTable(tableCatalogPath, metastoreTable, ignoreIfExists);
        } else {
            // Table does not exist on filesystem, we have to create a new _delta_log
            Metadata metadata = Metadata.builder()
                .schema(ddlDeltaSchema)
                .partitionColumns(ddlPartitionColumns)
                .configuration(filteredDdlOptions)
                .name(tableCatalogPath.getObjectName())
                .build();

            // create _delta_log
            DeltaCatalogTableHelper.commitToDeltaLog(
                deltaLog,
                metadata,
                Operation.Name.CREATE_TABLE
            );

            DeltaMetastoreTable metastoreTable =
                DeltaCatalogTableHelper.prepareMetastoreTable(table, deltaTablePath);

            // add table to metastore
            this.decoratedCatalog.createTable(tableCatalogPath, metastoreTable, ignoreIfExists);
        }
    }

    public void alterTable(DeltaCatalogBaseTable newCatalogTable) throws CatalogException {

        // Flink's Default SQL dialect support ALTER statements ONLY for changing table name
        // (Catalog::renameTable(...) and for changing/setting table properties. Schema/partition
        // change for Flink default SQL dialect is not supported.
        Map<String, String> alterTableDdlOptions = newCatalogTable.getOptions();
        String deltaTablePath =
            alterTableDdlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());

        // DDL options validation
        DeltaCatalogTableHelper.validateDdlOptions(alterTableDdlOptions);

        // At this point what we should have in ddlOptions are only delta table
        // properties, connector type, table path and user defined options. We don't want to
        // store connector type or table path in _delta_log, so we will filter those.
        Map<String, String> deltaAlterTableDdlOptions =
            DeltaCatalogTableHelper.filterMetastoreDdlOptions(alterTableDdlOptions);

        DeltaLog deltaLog = DeltaLog.forTable(hadoopConf, deltaTablePath);
        Metadata originalMetaData = deltaLog.update().getMetadata();

        // Add new properties to metadata.
        // Throw if DDL Delta table properties override previously defined properties from
        // _delta_log.
        Map<String, String> deltaLogProperties =
            DeltaCatalogTableHelper.prepareDeltaTableProperties(
                deltaAlterTableDdlOptions,
                newCatalogTable.getTableCatalogPath(),
                originalMetaData,
                true // allowOverride = true
            );

        Metadata updatedMetadata = originalMetaData.copyBuilder()
            .configuration(deltaLogProperties)
            .build();

        // add properties to _delta_log
        DeltaCatalogTableHelper
            .commitToDeltaLog(deltaLog, updatedMetadata, Operation.Name.SET_TABLE_PROPERTIES);
    }
}
