package io.delta.flink.source.internal;

import java.util.List;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;

/**
 * This interface defines and API contract for Delta Source Builder following the Step Builder
 * pattern.
 * <p>
 * This interface defines so-called "mandatory steps" that defines mandatory parameters needed by
 * Delta Source. The order of mandatory steps is fixed. Every new mandatory step should be added at
 * the end of {@link MandatorySteps} chain in order to make migration easier.
 * <p>
 * After defining last mandatory step, user will have an option to build the Source or define
 * optional parameters.
 */
public interface DeltaSourceBuilderSteps {

    /**
     * This interfaces represents a "collection" of mandatory steps, where each step corresponds to
     * mandatory field needed to create new {@link DeltaSource} instance.
     */
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

    /**
     * This interface represents a last step for Delta source step builder implementation. The main
     * method in this interface is {@link BuildStep#buildForRowData()} which finalizes the builder
     * and creates an instance of {@link DeltaSource}. Other methods in this interface represents
     * non-mandatory parameters for {@link DeltaSource} and are optional.
     */
    interface BuildStep {

        // -------------- Public Options --------------
        BuildStep versionAsOf(long snapshotVersion);

        BuildStep timestampAsOf(long snapshotTimestamp);

        BuildStep startingVersion(long startingVersion);

        BuildStep startingTimestamp(long startingTimestamp);

        BuildStep updateCheckIntervalMillis(long updateCheckInterval);

        BuildStep ignoreDeletes(long ignoreDeletes);

        BuildStep ignoreChanges(long ignoreChanges);
        // --------------------------------------------


        // -------------- Non Public Options ----------
        BuildStep option(String optionName, String optionValue);

        BuildStep option(String optionName, boolean optionValue);

        BuildStep option(String optionName, int optionValue);

        BuildStep option(String optionName, long optionValue);
        // --------------------------------------------

        BuildStep partitions(List<String> partitions);

        BuildStep continuousMode();

        // -------------- Finalizing methods ----------

        /**
         * Builds a {@link DeltaSource} instance for
         * {@link org.apache.flink.connector.file.src.reader.BulkFormat}
         * and with {@link RowData} as a type of produced records.
         */
        DeltaSource<RowData> buildForRowData();
    }

}
