package io.delta.flink.utils.extensions.hive;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Base class for JUnit Extensions that require a Hive Metastore database configuration pre-set.
 *
 * @implNote This class is extracted from https://github.com/ExpediaGroup/beeju/blob/beeju-5.0.0 and
 * "trimmed" to our needs. We could not use entire beeju library as sbt dependency due to Dependency
 * conflicts with Flink on Calcite, Parquet and many others. See https://github
 * .com/ExpediaGroup/beeju/issues/54
 * for details. As a result we added only org .apache.hive hive-exec and hive-metastore
 * dependencies, and we used beeju's Junit5 extension classes.
 */
public abstract class HiveJUnitExtension implements BeforeEachCallback, AfterEachCallback {

    protected HiveServerContext core;

    public HiveJUnitExtension(String databaseName, Map<String, String> configuration) {
        core = new HiveServerContext(databaseName, configuration);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        createDatabase(databaseName());
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        core.cleanUp();
    }

    /**
     * @return {@link HiveServerContext#driverClassName()}.
     */
    public String driverClassName() {
        return core.driverClassName();
    }

    /**
     * @return {@link HiveServerContext#databaseName()}.
     */
    public String databaseName() {
        return core.databaseName();
    }

    /**
     * @return {@link HiveServerContext#connectionURL()}
     */
    public String connectionURL() {
        return core.connectionURL();
    }

    /**
     * @return {@link HiveServerContext#conf()}.
     */
    public HiveConf conf() {
        return core.conf();
    }

    /**
     * @return {@link HiveServerContext#newClient()}.
     */
    public HiveMetaStoreClient newClient() {
        return core.newClient();
    }

    /**
     * See {@link HiveServerContext#createDatabase(String)}
     *
     * @param databaseName Database name.
     * @throws TException If an error occurs creating the database.
     */
    public void createDatabase(String databaseName) throws TException {
        core.createDatabase(databaseName);
    }

    /**
     * @return Root temporary directory as a file.
     */
    public File getTempDirectory() {
        return core.tempDir().toFile();
    }

    /**
     * @return Root warehouse directory as a file.
     */
    public File getWarehouseDirectory() {
        return core.warehouseDir().toFile();
    }
}
