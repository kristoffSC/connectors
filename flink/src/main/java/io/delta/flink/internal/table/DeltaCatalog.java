package io.delta.flink.internal.table;

import java.util.List;
import java.util.Optional;

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
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

public class DeltaCatalog extends AbstractCatalog {

    private final Catalog decoratedCatalog;

    public DeltaCatalog(String name, String defaultDatabase) {
        super(name, defaultDatabase);
        this.decoratedCatalog = new GenericInMemoryCatalog(name, defaultDatabase);
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
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
        throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        System.out.println("Create Table " + tablePath.getFullName());
        this.decoratedCatalog.createTable(tablePath, table, ignoreIfExists);
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
        return Optional.of(new DeltaDynamicTableFactory());
    }
}
