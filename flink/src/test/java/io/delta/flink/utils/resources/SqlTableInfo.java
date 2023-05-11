package io.delta.flink.utils.resources;

/**
 * Provides information about Table path, schema, record count and SQL schema for predefined Delta
 * tables used in Integration tests.
 */
public interface SqlTableInfo extends TableInfo {

    /**
     * @return Flink DDL table schema definition that can be used in Flink CREATE TABLE statement.
     * For example:
     * <pre>
     * String tableDdl = String.format("CREATE TABLE sourceTable (%s) WITH
     * ('connector' = 'delta','table-path' = '/some/path'), sourceTable.getSqlTableSchema());
     * </pre>
     * Where {@code sourceTable.getSqlTableSchema()} returns String like:
     * <pre>
     *      {col1 VARCHAR, col2 VARCHAR}
     * </pre>
     */
    String getSqlTableSchema();
}
