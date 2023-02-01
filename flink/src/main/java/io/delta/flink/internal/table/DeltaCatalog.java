package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import io.delta.flink.internal.ConnectorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FactoryUtil;
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

        // ------------------ Processing Delta Table ---------------
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
        validateDdlOptions(ddlOptions);

        // At this point what we should have in ddlOptions are only delta table
        // properties, connector type, table path and user defined options. We don't want to
        // store connector type or table path in _delta_log, so we will filter those
        Map<String, String> deltaDdlOptions = filterMetastoreaDdlOptions(ddlOptions);

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
            validateDdlSchemaAndPartitionSpecMatchesDelta(
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
                prepareDeltaTableProperties(
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
                prepareMetastoreTable(table, deltaTablePath, ddlPartitionColumns);
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

            CatalogTable metastoreTable =
                prepareMetastoreTable(table, deltaTablePath, ddlPartitionColumns);

            // add table to metastore
            this.decoratedCatalog.createTable(tableCatalogPath, metastoreTable, ignoreIfExists);
        }
    }

    /**
     * Validate DDL Delta table properties if they match properties from _delta_log add new
     * properties to metadata.
     *
     * @param deltaDdlOptions DDL options that should be added to _delta_log.
     * @param tableCatalogPath
     * @param deltaMetadata
     * @return a map of deltaLogProperties that will have same properties than original metadata
     * plus new ones that were defined in DDL.
     */
    private Map<String, String> prepareDeltaTableProperties(
            Map<String, String> deltaDdlOptions,
            ObjectPath tableCatalogPath,
            Metadata deltaMetadata,
            boolean allowOverride) {

        Map<String, String> deltaLogProperties = new HashMap<>(deltaMetadata.getConfiguration());
        for (Entry<String, String> ddlOption : deltaDdlOptions.entrySet()) {
            String deltaLogPropertyValue =
                deltaLogProperties.putIfAbsent(ddlOption.getKey(), ddlOption.getValue());

            if (!allowOverride
                && deltaLogPropertyValue != null
                && !deltaLogPropertyValue.equalsIgnoreCase(ddlOption.getValue())) {
                // _delta_log contains property defined in ddl but with different value.
                throw CatalogExceptionHelper.ddlAndDeltaLogOptionMismatchException(
                    tableCatalogPath,
                    ddlOption,
                    deltaLogPropertyValue
                );
            }
        }
        return deltaLogProperties;
    }

    private void validateDdlSchemaAndPartitionSpecMatchesDelta(
            String deltaTablePath,
            ObjectPath tableCatalogPath,
            List<String> ddlPartitionColumns,
            StructType ddlDeltaSchema,
            Metadata deltaMetadata) {

        StructType deltaSchema = deltaMetadata.getSchema();
        if (!(ddlDeltaSchema.equals(deltaSchema)
            && ConnectorUtils.listEqualsIgnoreOrder(
            ddlPartitionColumns,
                deltaMetadata.getPartitionColumns()))) {
            throw CatalogExceptionHelper.deltaLogAndDdlSchemaMismatchException(
                tableCatalogPath,
                deltaTablePath,
                deltaMetadata,
                ddlDeltaSchema,
                ddlPartitionColumns
            );
        }
    }

    private Map<String, String> filterMetastoreaDdlOptions(Map<String, String> ddlOptions) {
        return ddlOptions.entrySet().stream()
            .filter(entry ->
                !(entry.getKey().contains(FactoryUtil.CONNECTOR.key())
                    || entry.getKey().contains(DeltaTableConnectorOptions.TABLE_PATH.key()))
            ).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private CatalogTable prepareMetastoreTable(CatalogBaseTable table, String deltaTablePath,
        List<String> ddlPartitionColumns) {
        // Store only path, table name and connector type in metastore.
        // For computed and meta columns are not supported.
        Map<String, String> optionsToStoreInMetastore = new HashMap<>();
        optionsToStoreInMetastore.put(FactoryUtil.CONNECTOR.key(),
            DeltaDynamicTableFactory.IDENTIFIER);
        optionsToStoreInMetastore.put(DeltaTableConnectorOptions.TABLE_PATH.key(),
            deltaTablePath);

        // TODO DC - Consult with TD and Scott watermark and primary key definitions.
        List<Pair<String, Expression>> watermarkSpec = table.getUnresolvedSchema()
            .getWatermarkSpecs()
            .stream()
            .map(wmSpec -> Pair.of(wmSpec.getColumnName(), wmSpec.getWatermarkExpression()))
            .collect(Collectors.toList());

        // Add watermark spec to schema.
        Builder schemaBuilder = Schema.newBuilder();
        for (Pair<String, Expression> watermark : watermarkSpec) {
            schemaBuilder.watermark(watermark.getKey(), watermark.getValue());
        }

        // Add primary key def to schema.
        table.getUnresolvedSchema()
            .getPrimaryKey()
            .map(
                unresolvedPrimaryKey -> Pair.of(unresolvedPrimaryKey.getConstraintName(),
                    unresolvedPrimaryKey.getColumnNames())
            )
            .ifPresent(
                primaryKey -> schemaBuilder.primaryKeyNamed(primaryKey.getKey(),
                    primaryKey.getValue())
            );

        // prepare catalog table to store in metastore. This table will have only selected
        // options from DDL and an empty schema.
        return CatalogTable.of(
            // don't store any schema in metastore, except watermark and primary key def.
            schemaBuilder.build(),
            table.getComment(),
            ddlPartitionColumns,
            optionsToStoreInMetastore
        );
    }

    private void validateDdlOptions(Map<String, String> ddlOptions) {
        for (String ddlOption : ddlOptions.keySet()) {

            // validate for Flink Job specific options in DDL
            if (DeltaFlinkJobSpecificOptions.JOB_OPTIONS.contains(ddlOption)) {
                throw CatalogExceptionHelper.jobSpecificOptionInDdlException(ddlOption);
            }

            // TODO DC - Add tests for this
            // validate for Delta log Store config and parquet config.
            if (ddlOption.startsWith("spark.") ||
                ddlOption.startsWith("delta.logStore") ||
                ddlOption.startsWith("io.delta") ||
                ddlOption.startsWith("parquet.")) {
                throw CatalogExceptionHelper.invalidOptionInDdl(ddlOption);
            }
        }
    }

    public void alterTable(DeltaCatalogBaseTable catalogTable, boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException {
        //this.decoratedCatalog.alterTable(tablePath, newTable, ignoreIfNotExists);

        // Flink's Default SQL dialect support ALTER statements ONLY for changing table name
        // (Catalog::renameTable(...) and for changing/setting table properties. Schema/partition
        // change for Flink default SQL dialect is not supported.

        Map<String, String> alterTableDdlOptions = catalogTable.getOptions();
        String deltaTablePath =
            alterTableDdlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());

        // DDL options validation
        validateDdlOptions(alterTableDdlOptions);

        // At this point what we should have in ddlOptions are only delta table
        // properties, connector type, table path and user defined options. We don't want to
        // store connector type or table path in _delta_log, so we will filter those.
        Map<String, String> deltaAlterTableDdlOptions =
            filterMetastoreaDdlOptions(alterTableDdlOptions);

        DeltaLog deltaLog = DeltaLog.forTable(hadoopConfiguration, deltaTablePath);
        Metadata originalMetaData = deltaLog.update().getMetadata();

        // Add new properties to metadata.
        // Throw if DDL Delta table properties override previously defined properties from
        // _delta_log.
        Map<String, String> deltaLogProperties =
            prepareDeltaTableProperties(
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

    public List<CatalogPartitionSpec> listPartitions(DeltaCatalogBaseTable catalogTable)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {

        Map<String, String> ddlOptions = catalogTable.getMetastoreTable().getOptions();
        String deltaTablePath = ddlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());


        /*DeltaLog.forTable(hadoopConfiguration, deltaTablePath)
                .update()
                .getMetadata()
                .getPartitionColumns()
                .stream()
                .map(new Function<String, CatalogPartitionSpec>() {
                    @Override
                    public CatalogPartitionSpec apply(String deltaPartition) {
                        new CatalogPartitionSpec()
                        return null;
                    }
                });*/

        return Collections.emptyList();
    }

    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec)
        throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
        CatalogException {
        return this.decoratedCatalog.listPartitions(tablePath, partitionSpec);
    }

    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath,
        List<Expression> filters)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return this.decoratedCatalog.listPartitionsByFilter(tablePath, filters);
    }

    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {
        return this.decoratedCatalog.getPartition(tablePath, partitionSpec);
    }

    public boolean partitionExists(
            DeltaCatalogBaseTable catalogTable,
            CatalogPartitionSpec partitionSpec)
        throws CatalogException {

        Map<String, String> ddlOptions = catalogTable.getMetastoreTable().getOptions();
        String deltaTablePath = ddlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());

        // TODO DC - discuss partition spec with TD and Scott -> partition values missing in
        //  Delta Metadata.

        /*DeltaLog.forTable(hadoopConfiguration, deltaTablePath)
            .update()
            .getMetadata()*/

        return false;
    }

    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        CatalogPartition partition, boolean ignoreIfExists)
        throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
        PartitionAlreadyExistsException, CatalogException {
        this.decoratedCatalog.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
    }

    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        this.decoratedCatalog.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
    }

    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        CatalogPartition newPartition, boolean ignoreIfNotExists)
        throws PartitionNotExistException, CatalogException {
        this.decoratedCatalog.alterPartition(tablePath, partitionSpec, newPartition,
            ignoreIfNotExists);
    }

    private boolean databaseExists(String databaseName) {
        return this.decoratedCatalog.databaseExists(databaseName);
    }
}
