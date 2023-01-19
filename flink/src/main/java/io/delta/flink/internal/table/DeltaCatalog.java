package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.flink.internal.ConnectorUtils;
import io.delta.flink.sink.internal.SchemaConverter;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
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
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.Operation.Name;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

public class DeltaCatalog extends AbstractCatalog {

    private final Catalog decoratedCatalog;

    private final Configuration hadoopConfiguration;

    DeltaCatalog(String name, String defaultDatabase) {
        super(name, defaultDatabase);

        // TODO DC - the concrete decorated catalog should be abstracted and injected.
        this.decoratedCatalog = new GenericInMemoryCatalog(name, defaultDatabase);
        this.hadoopConfiguration =
            HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
    }

    @Override
    public void open() throws CatalogException {
        this.decoratedCatalog.open();
    }

    @Override
    public void close() throws CatalogException {
        this.decoratedCatalog.close();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return this.decoratedCatalog.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        return this.decoratedCatalog.getDatabase(databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return this.decoratedCatalog.databaseExists(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
        throws DatabaseAlreadyExistException, CatalogException {
        this.decoratedCatalog.createDatabase(name, database, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
        throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        this.decoratedCatalog.dropDatabase(name, ignoreIfNotExists, cascade);

    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
        throws DatabaseNotExistException, CatalogException {
        this.decoratedCatalog.alterDatabase(name, newDatabase, ignoreIfNotExists);
    }

    @Override
    public List<String> listTables(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        return this.decoratedCatalog.listTables(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        return this.decoratedCatalog.listViews(databaseName);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {
        return this.decoratedCatalog.getTable(tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return this.decoratedCatalog.tableExists(tablePath);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException {
        this.decoratedCatalog.dropTable(tablePath, ignoreIfNotExists);
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
        throws TableNotExistException, TableAlreadyExistException, CatalogException {
        this.decoratedCatalog.renameTable(tablePath, newTableName, ignoreIfNotExists);
    }

    @Override
    public void createTable(
            ObjectPath catalogTablePath,
            CatalogBaseTable table,
            boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        checkNotNull(catalogTablePath);
        checkNotNull(table);

        if (!databaseExists(catalogTablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), catalogTablePath.getDatabaseName());
        }

        Map<String, String> ddlOptions = table.getOptions();

        String connectorType = ddlOptions.get(FactoryUtil.CONNECTOR.key());
        if (!"delta".equals(connectorType)) {
            // it's not a Delta table.
            this.decoratedCatalog.createTable(catalogTablePath, table, ignoreIfExists);
        } else {
            String deltaTablePath = ddlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());
            if (StringUtils.isNullOrWhitespaceOnly(deltaTablePath)) {
                throw new RuntimeException("Path to Delta table cannot be null or empty.");
            }

            // Get Partition spec from DDL;
            List<String> ddlPartitionColumns = ((CatalogTable) table).getPartitionKeys();

            // Get Delta schema from Flink DDL.
            StructType ddlDeltaSchema= resolveDeltaSchemaFromDdl((ResolvedCatalogTable) table);
            DeltaLog deltaLog = DeltaLog.forTable(hadoopConfiguration, deltaTablePath);
            if (deltaLog.tableExists()) {
                // Table exists on filesystem, now we need to check if table exists in Metastore.
                if (this.decoratedCatalog.tableExists(catalogTablePath)) {
                    throw new TableAlreadyExistException(getName(), catalogTablePath);
                }

                StructType deltaSchema = deltaLog.update().getMetadata().getSchema();
                // TODO DC - handle case when deltaSchema is null.
                if (ddlDeltaSchema.equals(deltaSchema)) {
                    // TODO DC - validate Delta table properties here if they match _delta_log
                    // TODO DC - add properties to _delta_log
                    this.decoratedCatalog.createTable(catalogTablePath, table, ignoreIfExists);
                } else {
                    throw new RuntimeException(
                        String.format(
                            " Delta table [%s] from filesystem path [%s] has different schema "
                                + "that was defined in CREATE TABLE DDL.\n"
                                + "DDL schema [%s],\n"
                                + "_delta_log schema [%s]",
                            catalogTablePath,
                            deltaTablePath,
                            ddlDeltaSchema.getTreeString(),
                            deltaSchema.getTreeString())
                    );
                }
            } else {
                // Table does not exist on filesystem, we have to create new _delta_log
                OptimisticTransaction transaction = deltaLog.startTransaction();

                // TODO DC - add properties to metadata.
                Metadata metaDataAction = Metadata.builder()
                    .schema(ddlDeltaSchema)
                    .partitionColumns(ddlPartitionColumns)
                    .build();
                transaction.updateMetadata(metaDataAction);
                Operation opName = prepareDeltaLogOperation(Name.CREATE_TABLE, metaDataAction);
                transaction.commit(
                    Collections.singletonList(metaDataAction),
                    opName,
                    ConnectorUtils.ENGINE_INFO
                );
                this.decoratedCatalog.createTable(catalogTablePath, table, ignoreIfExists);
            }

        }
        System.out.println("Create Table " + catalogTablePath.getFullName());
    }

    private StructType resolveDeltaSchemaFromDdl(ResolvedCatalogTable table) {

        // contains physical, computed and metadata columns that were defined in DDL
        List<Column> columns = table.getResolvedSchema().getColumns();
        validateDuplicateColumns(columns);

        List<String> names = new LinkedList<>();
        List<LogicalType> types = new LinkedList<>();

        for (Column column : columns) {
            if (column instanceof PhysicalColumn || column instanceof ComputedColumn) {
                names.add(column.getName());
                types.add(column.getDataType().getLogicalType());
            }
        }

        return SchemaConverter.toDeltaDataType(
            RowType.of(types.toArray(new LogicalType[0]), names.toArray(new String[0]))
        );
    }

    private void validateDuplicateColumns(List<Column> columns) {
        final List<String> names =
            columns.stream().map(Column::getName).collect(Collectors.toList());
        final List<String> duplicates =
            names.stream()
                .filter(name -> Collections.frequency(names, name) > 1)
                .distinct()
                .collect(Collectors.toList());
        if (duplicates.size() > 0) {
            throw new ValidationException(
                String.format(
                    "Schema must not contain duplicate column names. Found duplicates: %s",
                    duplicates));
        }
    }

    /**
     * Prepares {@link Operation} object for current transaction
     *
     * @param opName name of the operation.
     * @param metadata Delta Table Metadata action.
     * @return {@link Operation} object for current transaction.
     */
    private Operation prepareDeltaLogOperation(Name opName, Metadata metadata) {
        Map<String, String> operationParameters = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // TODO DC - consult with Scott this "mode" thing. This is what Sink's
            //  GlobalCommitter does.
            //operationParameters.put("mode", objectMapper.writeValueAsString(APPEND_MODE));
            // we need to perform mapping to JSON object twice for partition columns. First to map
            // the list to string type and then again to make this string JSON encoded
            // e.g. java array of ["a", "b"] will be mapped as string "[\"a\",\"c\"]"
            operationParameters.put("isManaged", objectMapper.writeValueAsString(false));
            operationParameters.put("description",
                objectMapper.writeValueAsString(metadata.getDescription()));

            // TODO DC - consult this with Scott, Delta seems to expect "[]" and "{}" rather then
            //  [] and {}.
            operationParameters.put("properties",
                objectMapper.writeValueAsString(
                    objectMapper.writeValueAsString(metadata.getConfiguration())));
            operationParameters.put("partitionBy", objectMapper.writeValueAsString(
                objectMapper.writeValueAsString(metadata.getPartitionColumns())));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot map object to JSON", e);
        }
        return new Operation(opName, operationParameters, Collections.emptyMap());
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable,
        boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        this.decoratedCatalog.alterTable(tablePath, newTable, ignoreIfNotExists);
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
    public List<String> listFunctions(String dbName)
        throws DatabaseNotExistException, CatalogException {
        return this.decoratedCatalog.listFunctions(dbName);
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
        throws FunctionNotExistException, CatalogException {
        return this.decoratedCatalog.getFunction(functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return this.decoratedCatalog.functionExists(functionPath);
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function,
        boolean ignoreIfExists)
        throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        this.decoratedCatalog.createFunction(functionPath, function, ignoreIfExists);
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction,
        boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        this.decoratedCatalog.alterFunction(functionPath, newFunction, ignoreIfNotExists);
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
        throws FunctionNotExistException, CatalogException {
        this.decoratedCatalog.dropFunction(functionPath, ignoreIfNotExists);
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

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(DeltaDynamicTableFactory.fromCatalog());
    }
}
