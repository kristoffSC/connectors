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

import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * Implementation of {@link ScanTableSource} interface for Table/SQL support for Delta Source
 * connector.
 */
public class DeltaDynamicTableSource implements ScanTableSource {

    private final Configuration hadoopConf;

    private final ReadableConfig tableOptions;

    private final RowType rowType;

    /**
     * Constructor for creating Source of Flink dynamic table to Delta table.
     *
     * @param hadoopConf   Hadoop's configuration.
     * @param tableOptions Table options returned by Catalog and resolved query plan.
     * @param rowType      Flink's logical type with the structure of the events in the stream.
     */
    public DeltaDynamicTableSource(
            Configuration hadoopConf,
            ReadableConfig tableOptions,
            RowType rowType) {

        this.hadoopConf = hadoopConf;
        this.tableOptions = tableOptions;
        this.rowType = rowType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        String mode = tableOptions.get(DeltaFlinkJobSpecificOptions.MODE);
        String tablePath = tableOptions.get(DeltaTableConnectorOptions.TABLE_PATH);

        DeltaSourceBuilderBase<RowData, ?> sourceBuilder;

        if (DeltaFlinkJobSpecificOptions.MODE.defaultValue().equalsIgnoreCase(mode)) {
            sourceBuilder = DeltaSource.forBoundedRowData(new Path(tablePath), hadoopConf);
        } else {
            sourceBuilder = DeltaSource.forContinuousRowData(new Path(tablePath), hadoopConf);
        }

        sourceBuilder
            .columnNames(rowType.getFieldNames());

        return SourceProvider.of(sourceBuilder.build());
    }

    @Override
    public DynamicTableSource copy() {
        return new DeltaDynamicTableSource(this.hadoopConf, this.tableOptions, this.rowType);
    }

    @Override
    public String asSummaryString() {
        return "DeltaSource";
    }

    @VisibleForTesting
    Configuration getHadoopConf() {
        return new Configuration(this.hadoopConf);
    }
}
