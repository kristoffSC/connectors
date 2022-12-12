package io.delta.flink.internal.table;

import java.util.List;

import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

public class DeltaDynamicTableSource implements
        ScanTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsLimitPushDown {

    private final Configuration hadoopConf;

    private final ReadableConfig tableOptions;

    private final RowType rowType;

    private long limit;

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
            // TODO FLINK_SQL_PR2 write IT test for this.
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

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // TODO FLINK_SQL_PR2 Support filters
        return null;
    }

    @Override
    public void applyLimit(long limit) {
        // TODO FLINK_SQL_PR2 use this
        this.limit = limit;

    }

    @Override
    public boolean supportsNestedProjection() {
        /// TODO FLINK_SQL_PR2  support nested projection
        return false;
    }
}
