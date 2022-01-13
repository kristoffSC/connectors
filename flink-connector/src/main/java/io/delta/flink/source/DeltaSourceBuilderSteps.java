package io.delta.flink.source;

import java.util.List;

import org.apache.flink.connector.file.src.ContinuousEnumerationSettings;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;

public interface DeltaSourceBuilderSteps {

    interface MandatorySteps
        extends TablePathStep, TableColumnNamesStep, TableColumnTypesStep, ConfigurationStep {

    }


    interface TablePathStep {

        TableColumnNamesStep tablePath(Path tablePath);
    }

    interface TableColumnNamesStep {

        TableColumnTypesStep columnNames(String[] columnNames);
    }

    interface TableColumnTypesStep {

        ConfigurationStep columnTypes(LogicalType[] columnTypes);
    }

    interface ConfigurationStep {

        BuildStep configuration(Configuration configuration);
    }

    interface BuildStep {

        BuildStep partitions(List<String> partitions);

        BuildStep continuousEnumerationSettings(
            ContinuousEnumerationSettings continuousEnumerationSettings);

        DeltaSource<RowData> build();
    }

}
