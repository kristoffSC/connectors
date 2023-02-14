package io.delta.flink.utils.extensions;

import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;

public abstract class BaseCatalogExtension implements BeforeEachCallback, AfterEachCallback {

    public abstract void setupDeltaCatalog(TableEnvironment tableEnv);

}
