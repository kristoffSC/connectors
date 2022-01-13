package io.delta.flink.source;
import java.util.List;

import io.delta.flink.source.DeltaSourceBuilderSteps.BuildStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.ConfigurationStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.MandatorySteps;
import io.delta.flink.source.DeltaSourceBuilderSteps.TableColumnNamesStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.TableColumnTypesStep;
import io.delta.flink.source.DeltaSourceBuilderSteps.TablePathStep;
import io.delta.flink.source.enumerator.BoundedSplitEnumeratorProvider;
import io.delta.flink.source.enumerator.ContinuousSplitEnumeratorProvider;
import io.delta.flink.source.file.AddFileEnumerator;
import io.delta.flink.source.state.DeltaSourceSplit;
import org.apache.flink.connector.file.src.ContinuousEnumerationSettings;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;


public final class DeltaSourceBuilder {

    public static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
        LocalityAwareSplitAssigner::new;

    public static final AddFileEnumerator.Provider<DeltaSourceSplit>
        DEFAULT_SPLITTABLE_FILE_ENUMERATOR = DeltaFileEnumerator::new;

    private static final BoundedSplitEnumeratorProvider DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER =
        new BoundedSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    private static final ContinuousSplitEnumeratorProvider
        DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER =
        new ContinuousSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    private DeltaSourceBuilder() {

    }

    public static TablePathStep builder() {
        return new BuildSteps();
    }

    private static ParquetColumnarRowInputFormat<DeltaSourceSplit> buildFormat(String[] columnNames,
        LogicalType[] columnTypes, Configuration configuration) {

        return new ParquetColumnarRowInputFormat<>(
            configuration,
            RowType.of(columnTypes, columnNames),
            500,
            false,
            true
        );
    }

    private static ParquetColumnarRowInputFormat<DeltaSourceSplit> buildPartitionedFormat(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration,
        List<String> partitionKeys) {

        return ParquetColumnarRowInputFormat.createPartitionedFormat(
            configuration,
            RowType.of(columnTypes, columnNames),
            partitionKeys, new DeltaPartitionFieldExtractor<>(),
            500,
            false,
            true);
    }

    private static class BuildSteps implements MandatorySteps, BuildStep {

        private Path tablePath;
        private String[] columnNames;
        private LogicalType[] columnTypes;
        private Configuration configuration;
        private ContinuousEnumerationSettings continuousEnumerationSettings;
        private List<String> partitions;

        @Override
        public TableColumnNamesStep tablePath(Path tablePath) {
            this.tablePath = tablePath;
            return this;
        }

        @Override
        public TableColumnTypesStep columnNames(String[] columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        @Override
        public ConfigurationStep columnTypes(LogicalType[] columnTypes) {
            this.columnTypes = columnTypes;
            return this;
        }

        @Override
        public BuildStep configuration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        @Override
        public BuildStep partitions(List<String> partitions) {
            this.partitions = partitions;
            return this;
        }

        @Override
        public BuildStep continuousEnumerationSettings(
            ContinuousEnumerationSettings continuousEnumerationSettings) {
            this.continuousEnumerationSettings = continuousEnumerationSettings;
            return this;
        }

        @Override
        public DeltaSource<RowData> build() {

            ParquetColumnarRowInputFormat<DeltaSourceSplit> format;
            if (partitions == null || partitions.isEmpty()) {
                format = buildFormat(columnNames, columnTypes, configuration);
            } else {
                format =
                    buildPartitionedFormat(columnNames, columnTypes, configuration, partitions);
            }

            // TODO pass continuousEnumerationSettings to
            //  DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER
            return DeltaSource.forBulkFileFormat(tablePath, format,
                (continuousEnumerationSettings == null)
                    ? DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER
                    : DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER,
                configuration);
        }
    }
}
