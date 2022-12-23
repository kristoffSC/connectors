package io.delta.flink.internal.table;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.connector.datagen.table.DataGenTableSource;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeltaDynamicTableFactoryTest {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaDynamicTableFactoryTest.class);

    public static final ResolvedSchema SCHEMA =
        ResolvedSchema.of(
            Column.physical("a", DataTypes.STRING()),
            Column.physical("b", DataTypes.INT()),
            Column.physical("c", DataTypes.BOOLEAN()));

    private DeltaDynamicTableFactory tableFactory;

    private Map<String, String> options;

    @BeforeEach
    public void setUp() {
        this.tableFactory = new DeltaDynamicTableFactory();
        this.options = new HashMap<>();
        this.options.put(FactoryUtil.CONNECTOR.key(), "delta");
    }

    @Test
    void shouldLoadHadoopConfFromPath() {

        String path = "src/test/resources/hadoop-conf";
        File file = new File(path);
        String confDir = file.getAbsolutePath();

        options.put("table-path", "file://some/path");
        options.put("hadoop-conf-dir", confDir);
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        DeltaDynamicTableSource dynamicTableSource =
            (DeltaDynamicTableSource) tableFactory.createDynamicTableSource(tableContext);

        DeltaDynamicTableSink dynamicTableSink =
            (DeltaDynamicTableSink) tableFactory.createDynamicTableSink(tableContext);

        assertHadoopConf(dynamicTableSource.getHadoopConf());
        assertHadoopConf(dynamicTableSink.getHadoopConf());
    }

    @Test
    void shouldLoadHadoopConfFromHadoopHomeEnv() {

        String path = "src/test/resources/hadoop-conf";
        File file = new File(path);
        String confDir = file.getAbsolutePath();

        options.put("table-path", "file://some/path");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        CommonTestUtils.setEnv(Collections.singletonMap("HADOOP_HOME", confDir), true);

        DeltaDynamicTableSource dynamicTableSource =
            (DeltaDynamicTableSource) tableFactory.createDynamicTableSource(tableContext);

        DeltaDynamicTableSink dynamicTableSink =
            (DeltaDynamicTableSink) tableFactory.createDynamicTableSink(tableContext);

        Configuration sourceHadoopConf = dynamicTableSink.getHadoopConf();
        assertThat(
            sourceHadoopConf.get("dummy.property1", "noValue_asDefault"), equalTo("false-value"))
        ;
        assertThat(
            sourceHadoopConf.get("dummy.property2", "noValue_asDefault"), equalTo("11")
        );

        Configuration sinkHadoopConf = dynamicTableSink.getHadoopConf();
        assertThat(
            sinkHadoopConf.get("dummy.property1", "noValue_asDefault"), equalTo("false-value")
        );
        assertThat(
            sinkHadoopConf.get("dummy.property2", "noValue_asDefault"), equalTo("11")
        );
    }

    @Test
    void shouldLoadHadoopConfFromHadoopConfDirEnv() {

        String path = "src/test/resources/hadoop-conf";
        File file = new File(path);
        String confDir = file.getAbsolutePath();

        options.put("table-path", "file://some/path");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        CommonTestUtils.setEnv(Collections.singletonMap("HADOOP_CONF_DIR", confDir), true);

        DeltaDynamicTableSource dynamicTableSource =
            (DeltaDynamicTableSource) tableFactory.createDynamicTableSource(tableContext);

        DeltaDynamicTableSink dynamicTableSink =
            (DeltaDynamicTableSink) tableFactory.createDynamicTableSink(tableContext);

        assertHadoopConf(dynamicTableSource.getHadoopConf());
        assertHadoopConf(dynamicTableSink.getHadoopConf());
    }

    @Test
    void shouldOverrideConfFromConfDirProperty() {

        String path = "src/test/resources/hadoop-conf";
        File file = new File(path);
        String confDir = file.getAbsolutePath();

        options.put("table-path", "file://some/path");
        options.put("hadoop-conf-dir", confDir);
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        CommonTestUtils.setEnv(Collections.singletonMap("HADOOP_HOME", confDir), true);

        DeltaDynamicTableSource dynamicTableSource =
            (DeltaDynamicTableSource) tableFactory.createDynamicTableSource(tableContext);

        DeltaDynamicTableSink dynamicTableSink =
            (DeltaDynamicTableSink) tableFactory.createDynamicTableSink(tableContext);

        assertHadoopConf(dynamicTableSource.getHadoopConf());
        assertHadoopConf(dynamicTableSink.getHadoopConf());
    }

    @Test
    void shouldValidateMissingTablePathOption() {

        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, Collections.emptyMap());

        ValidationException sinkValidationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        ValidationException sourceValidationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSource(tableContext)
        );

        LOG.info(sinkValidationException.getMessage());
        LOG.info(sourceValidationException.getMessage());
    }

    @Test
    void shouldValidateUsedUnexpectedOption() {

        options.put("table-path", "file://some/path");
        options.put("invalid-Option", "MyTarget");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        ValidationException sinkValidationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        ValidationException sourceValidationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSource(tableContext)
        );

        LOG.info(sinkValidationException.getMessage());
        LOG.info(sourceValidationException.getMessage());
    }

    @Test
    void shouldValidateIfMissingHadoopConfDir() {

        options.put("table-path", "file://some/path");
        options.put("hadoop-conf-dir", "fiele://invalid/path");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        RuntimeException sinkValidationException = assertThrows(
            RuntimeException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        RuntimeException sourceValidationException = assertThrows(
            RuntimeException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        assertThat(
            sinkValidationException.getCause().getClass(), equalTo(FileNotFoundException.class)
        );
        assertThat(
            sourceValidationException.getCause().getClass(), equalTo(FileNotFoundException.class)
        );

        LOG.info(sinkValidationException.getMessage());
        LOG.info(sourceValidationException.getMessage());
    }

    @Test
    public void shouldReturnNonDeltaSourceFactory() {

        this.options.put(FactoryUtil.CONNECTOR.key(), "datagen");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        DynamicTableSource dynamicTableSource =
            tableFactory.createDynamicTableSource(tableContext);

        assertThat(dynamicTableSource.getClass(), equalTo(DataGenTableSource.class));
    }

    @Test
    public void shouldReturnNonDeltaSinkFactory() {

        this.options.put(FactoryUtil.CONNECTOR.key(), "blackhole");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        DynamicTableSink dynamicTableSink =
            tableFactory.createDynamicTableSink(tableContext);

        assertThat(dynamicTableSink.asSummaryString(), equalTo("BlackHole"));
    }

    private void assertHadoopConf(Configuration actualConf ) {
        assertThat(actualConf.get("dummy.property1", "noValue_asDefault"), equalTo("false"));
        assertThat(actualConf.get("dummy.property2", "noValue_asDefault"), equalTo("1"));
    }
}
