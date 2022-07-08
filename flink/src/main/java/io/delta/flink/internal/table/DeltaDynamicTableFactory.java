/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.flink.internal.table;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * Creates a {@link DynamicTableSource} instance representing DeltaLake table.
 *
 * <p>
 * This implementation automatically resolves all necessary object for creating instance of {@link
 * io.delta.flink.sink.DeltaSink} except Delta table's path that needs to be provided explicitly.
 */
public class DeltaDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "delta";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig tableOptions = helper.getOptions();
        ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();

        Configuration conf = resolveHadoopConf(tableOptions);
        RowType rowType = (RowType) tableSchema.toSinkRowDataType().getLogicalType();

        Boolean shouldTryUpdateSchema = tableOptions
            .getOptional(DeltaTableConnectorOptions.SHOULD_TRY_UPDATE_SCHEMA)
            .orElse(DeltaTableConnectorOptions.SHOULD_TRY_UPDATE_SCHEMA.defaultValue());

        return new DeltaDynamicTableSink(
            new Path(tableOptions.get(DeltaTableConnectorOptions.TABLE_PATH)),
            conf,
            rowType,
            shouldTryUpdateSchema,
            context.getCatalogTable()
        );
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DeltaTableConnectorOptions.TABLE_PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DeltaTableConnectorOptions.HADOOP_CONF_DIR);
        options.add(DeltaTableConnectorOptions.SHOULD_TRY_UPDATE_SCHEMA);
        return options;
    }

    /**
     * Tries to resolve Hadoop conf either from conf dir provided as table option or from env
     * variable.
     *
     * @param tableOptions Flink Table's options resolved for given table
     * @return {@link Configuration} object
     */
    private Configuration resolveHadoopConf(ReadableConfig tableOptions) {
        Configuration conf = null;
        Optional<String> hadoopConfDirOptional =
            tableOptions.getOptional(DeltaTableConnectorOptions.HADOOP_CONF_DIR);
        if (hadoopConfDirOptional.isPresent()) {
            conf = getHadoopConfiguration(hadoopConfDirOptional.get());
            if (conf == null) {
                throw new RuntimeException(
                    "Failed to resolve Hadoop conf file from given path",
                    new FileNotFoundException(
                        "Couldn't resolve Hadoop conf at given path: " +
                            hadoopConfDirOptional.get()));
            }
        } else if (System.getenv("HADOOP_CONF_DIR") != null) {
            conf = getHadoopConfiguration(System.getenv("HADOOP_CONF_DIR"));
        }

        if (conf == null) {
            conf = new Configuration();
        }
        conf.set("parquet.compression", "SNAPPY");
        return conf;
    }

    /**
     * Returns a new Hadoop Configuration object using the path to the hadoop conf configured.
     *
     * @param hadoopConfDir Hadoop conf directory path.
     * @return A Hadoop configuration instance.
     */
    private Configuration getHadoopConfiguration(String hadoopConfDir) {
        if (new File(hadoopConfDir).exists()) {
            List<File> possibleConfFiles = new ArrayList<>();
            List<String> possibleConfigs = Arrays.asList(
                "core-site.xml",
                "hdfs-site.xml",
                "yarn-site.xml",
                "mapred-site.xml"
            );
            for (String confPath : possibleConfigs) {
                File confFile = new File(hadoopConfDir, confPath);
                if (confFile.exists()) {
                    possibleConfFiles.add(confFile);
                }
            }

            if (!possibleConfFiles.isEmpty()) {
                Configuration hadoopConfiguration = new Configuration();
                for (File confFile : possibleConfFiles) {
                    hadoopConfiguration.addResource(
                        new org.apache.hadoop.fs.Path(confFile.getAbsolutePath()));
                }
                return hadoopConfiguration;
            }
        }
        return null;
    }
}
