package io.delta.flink.source;

import java.util.List;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;

public interface DeltaSourceBuilderSteps {

    interface MandatorySteps
        extends TablePathStep, TableColumnNamesStep, TableColumnTypesStep, HadoopConfigurationStep {

    }


    interface TablePathStep {

        TableColumnNamesStep tablePath(Path tablePath);
    }

    interface TableColumnNamesStep {

        TableColumnTypesStep columnNames(String[] columnNames);
    }

    interface TableColumnTypesStep {

        HadoopConfigurationStep columnTypes(LogicalType[] columnTypes);
    }

    interface HadoopConfigurationStep {

        BuildStep hadoopConfiguration(Configuration configuration);
    }

    interface BuildStep {

        BuildStep option(String optionName, String optionValue);

        BuildStep option(String optionName, boolean optionValue);

        BuildStep option(String optionName, int optionValue);

        BuildStep option(String optionName, long optionValue);

        BuildStep partitions(List<String> partitions);

        BuildStep continuousMode();

        DeltaSource<RowData> build();
    }

}
