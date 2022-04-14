package io.delta.flink.source.builder;

import java.util.List;

import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.DeltaSourceInternal;
import org.apache.flink.core.fs.Path;
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
    interface MandatorySteps<T>
        extends TablePathStep<T>, TableColumnNameStep<T>, TableColumnTypeStep<T>,
        HadoopConfigurationStep<T> {

    }

    /**
     * This interface represents a builder step for {@link DeltaSource} to set Delta table path.
     */
    interface TablePathStep<T> {

        /**
         * Sets {@link Path} to Delta table.
         */
        TableColumnNameStep<T> tablePath(Path tablePath);
    }

    /**
     * This interface represents a builder step for {@link DeltaSource} to define column names that
     * should be read from Delta table.
     */
    interface TableColumnNameStep<T> {

        TableColumnTypeStep<T> columnNames(String[] columnNames);
    }

    /**
     * This interface represents a builder step for {@link DeltaSource} to define column types that
     * should be read from Delta table.
     */
    interface TableColumnTypeStep<T> {

        HadoopConfigurationStep<T> columnTypes(LogicalType[] columnTypes);
    }

    /**
     * This interface represents a builder step for {@link DeltaSource} to define a Hadoop
     * configuration that should be for reading Delta table.
     */
    interface HadoopConfigurationStep<T> {

        BuildStep<T> hadoopConfiguration(Configuration configuration);
    }

    /**
     * This interface represents a last step for Delta source step builder implementation. The main
     * method in this interface is {@link BuildStep#build()} which finalizes the builder and creates
     * an instance of {@link DeltaSource}. Other methods in this interface represents
     * non-mandatory parameters for {@link DeltaSource} and are optional.
     */
    interface BuildStep<T> {

        // -------------- Public Options --------------
        BuildStep<T> versionAsOf(long snapshotVersion);

        BuildStep<T> timestampAsOf(long snapshotTimestamp);

        BuildStep<T> startingVersion(long startingVersion);

        BuildStep<T> startingTimestamp(long startingTimestamp);

        BuildStep<T> updateCheckIntervalMillis(long updateCheckInterval);

        BuildStep<T> ignoreDeletes(long ignoreDeletes);

        BuildStep<T> ignoreChanges(long ignoreChanges);
        // --------------------------------------------


        // -------------- Non Public Options ----------
        BuildStep<T> option(String optionName, String optionValue);

        BuildStep<T> option(String optionName, boolean optionValue);

        BuildStep<T> option(String optionName, int optionValue);

        BuildStep<T> option(String optionName, long optionValue);
        // --------------------------------------------

        BuildStep<T> partitions(List<String> partitions);

        BuildStep<T> continuousMode();

        // -------------- Finalizing methods ----------

        <V extends DeltaSourceInternal<T>> V build();
    }

}
