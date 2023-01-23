package io.delta.flink.internal.table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import io.delta.flink.internal.ConnectorUtils;
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
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.StringUtils;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation.Name;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

public class DeltaCatalog extends DeltaCatalogBase {

    DeltaCatalog(String name, String defaultDatabase) {
        super(name, defaultDatabase);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
        return this.decoratedCatalog.getTable(tablePath);
    }

    // TODO DC - should we check both, filesystem and metastore or should we check only metastore?
    //  If latter, then what about "transaction" in create table and case when exception occurred
    //  after storing in metastore but before or during creating _delta_log on filesystem.
    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return this.decoratedCatalog.tableExists(tablePath);
    }

    @Override
    public void createTable(
            ObjectPath catalogTablePath,
            CatalogBaseTable table,
            boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        checkNotNull(catalogTablePath);
        checkNotNull(table);

        Map<String, String> ddlOptions = table.getOptions();
        String connectorType = ddlOptions.get(FactoryUtil.CONNECTOR.key());
        if (!DeltaDynamicTableFactory.IDENTIFIER.equals(connectorType)) {
            // it's not a Delta table, redirect to decorated catalog.
            this.decoratedCatalog.createTable(catalogTablePath, table, ignoreIfExists);
            return;
        }

        // ------------------ Processing Delta Table ---------------
        if (!databaseExists(catalogTablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), catalogTablePath.getDatabaseName());
        }

        String deltaTablePath = ddlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());
        if (StringUtils.isNullOrWhitespaceOnly(deltaTablePath)) {
            throw new CatalogException("Path to Delta table cannot be null or empty.");
        }

        // DDL options validation
        for (String ddlOption : ddlOptions.keySet()) {

            // validate for Flink Job specific options in DDL
            if (DeltaFlinkJobSpecificOptions.JOB_OPTIONS.contains(ddlOption)) {
                throw CatalogExceptionHelper.jobSpecificOptionInDdlException(ddlOption);
            }

            // validate for Delta log Store config and parquet config.
            if (ddlOption.startsWith("spark.") ||
                ddlOption.startsWith("delta.logStore") ||
                ddlOption.startsWith("io.delta") ||
                ddlOption.startsWith("parquet.")) {
                throw CatalogExceptionHelper.invalidOptionInDdl(ddlOption);
            }
        }

        // At this point what we should have in ddlOptions are only delta table
        // properties, connector type, table path and user defined options.
        // We don't want to store connector type or table path in _delta_log
        Map<String, String> deltaDdlOptions = ddlOptions.entrySet().stream()
            .filter(entry ->
                !(entry.getKey().contains(FactoryUtil.CONNECTOR.key())
                    || entry.getKey().contains(DeltaTableConnectorOptions.TABLE_PATH.key()))
            ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Get Partition columns from DDL;
        List<String> ddlPartitionColumns = ((CatalogTable) table).getPartitionKeys();

        // Get Delta schema from Flink DDL.
        StructType ddlDeltaSchema =
            DeltaCatalogTableHelper.resolveDeltaSchemaFromDdl((ResolvedCatalogTable) table);

        DeltaLog deltaLog = DeltaLog.forTable(hadoopConfiguration, deltaTablePath);
        if (deltaLog.tableExists()) {
            // Table exists on filesystem, now we need to check if table exists in Metastore and
            // if so, throw exception.
            if (this.decoratedCatalog.tableExists(catalogTablePath)) {
                throw new TableAlreadyExistException(getName(), catalogTablePath);
            }

            // Table was not present in metastore however it is present on Filesystem, we have to
            // verify if schema, partition spec and properties stored in _delta_log match with DDL.
            // TODO DC - handle case when deltaSchema is null.
            Metadata deltaMetadata = deltaLog.update().getMetadata();
            StructType deltaSchema = deltaMetadata.getSchema();

            if (!(ddlDeltaSchema.equals(deltaSchema)
                || ConnectorUtils.listEqualsIgnoreOrder(
                    ddlPartitionColumns,
                    deltaMetadata.getPartitionColumns()))) {
                throw CatalogExceptionHelper.deltaLogAndDdlSchemaMismatchException(
                    catalogTablePath,
                    deltaTablePath,
                    deltaMetadata,
                    ddlDeltaSchema,
                    ddlPartitionColumns
                );
            }

            // validate DDL Delta table properties if they match properties from _delta_log
            // add new properties to metadata.
            Map<String, String> deltaLogProperties =
                new HashMap<>(deltaMetadata.getConfiguration());
            for (Entry<String, String> ddlOption : deltaDdlOptions.entrySet()) {

                String deltaLogPropertyValue =
                    deltaLogProperties.putIfAbsent(ddlOption.getKey(), ddlOption.getValue());

                if (deltaLogPropertyValue != null
                    && !deltaLogPropertyValue.equalsIgnoreCase(ddlOption.getValue())) {
                    // _delta_log contains property defined in ddl but with different value.
                    throw CatalogExceptionHelper.ddlAndDeltaLogOptionMismatchException(
                        catalogTablePath,
                        ddlOption,
                        deltaLogPropertyValue
                    );
                }
            }

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
            // TODO DC - store only path, table name and connector type in metastore
            //  analyze do we need to store schema... <- computed columns expression, metadata
            //  columns in the future.
            this.decoratedCatalog.createTable(catalogTablePath, table, ignoreIfExists);
        } else {
            // Table does not exist on filesystem, we have to create a new _delta_log
            Metadata metadata = Metadata.builder()
                .schema(ddlDeltaSchema)
                .partitionColumns(ddlPartitionColumns)
                .configuration(deltaDdlOptions)
                .build();

            DeltaCatalogTableHelper.commitToDeltaLog(deltaLog, metadata, Name.CREATE_TABLE);

            // TODO DC - store only path, table name and connector type in metastore
            //  analyze do we need to store schema... <- computed columns expression, metadata
            //  columns in the future.
            this.decoratedCatalog.createTable(catalogTablePath, table, ignoreIfExists);
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable,
        boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        this.decoratedCatalog.alterTable(tablePath, newTable, ignoreIfNotExists);
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
        return this.decoratedCatalog.getTableStatistics(tablePath);
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
        return this.decoratedCatalog.getTableColumnStatistics(tablePath);
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
        boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        this.decoratedCatalog.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath,
        CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException, TablePartitionedException {
        this.decoratedCatalog.alterTableColumnStatistics(tablePath, columnStatistics,
            ignoreIfNotExists);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return this.decoratedCatalog.listPartitions(tablePath);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec)
        throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
        CatalogException {
        return this.decoratedCatalog.listPartitions(tablePath, partitionSpec);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath,
        List<Expression> filters)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return this.decoratedCatalog.listPartitionsByFilter(tablePath, filters);
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {
        return this.decoratedCatalog.getPartition(tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws CatalogException {
        return this.decoratedCatalog.partitionExists(tablePath, partitionSpec);
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        CatalogPartition partition, boolean ignoreIfExists)
        throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
        PartitionAlreadyExistsException, CatalogException {
        this.decoratedCatalog.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        this.decoratedCatalog.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        CatalogPartition newPartition, boolean ignoreIfNotExists)
        throws PartitionNotExistException, CatalogException {
        this.decoratedCatalog.alterPartition(tablePath, partitionSpec, newPartition,
            ignoreIfNotExists);
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return this.decoratedCatalog.getPartitionStatistics(tablePath, partitionSpec);
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return this.decoratedCatalog.getPartitionColumnStatistics(tablePath, partitionSpec);
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
        CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
        throws PartitionNotExistException, CatalogException {
        this.decoratedCatalog.alterPartitionStatistics(tablePath, partitionSpec,
            partitionStatistics, ignoreIfNotExists);
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics,
        boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        this.decoratedCatalog.alterPartitionColumnStatistics(tablePath, partitionSpec,
            columnStatistics, ignoreIfNotExists);
    }
}