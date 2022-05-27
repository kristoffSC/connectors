package io.delta.flink.source;

import java.util.Arrays;
import java.util.List;

import io.delta.flink.source.internal.builder.ContinuousDeltaSourceBuilder;
import io.delta.flink.source.internal.builder.DeltaBulkFormat;
import io.delta.flink.source.internal.builder.RowDataFormat;
import io.delta.flink.source.internal.enumerator.supplier.ContinuousSnapshotSupplierFactory;
import io.delta.flink.source.internal.utils.SourceSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.INITIAL_SNAPSHOT_VERSION;

/**
 * A builder class for {@link DeltaSource} for a stream of {@link RowData}. Created source instance
 * will operate in {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
 * mode.
 * <p>
 * For most common use cases use {@link DeltaSource#forContinuousRowData} utility method to
 * instantiate the source. After instantiation of this builder you can either call {@link
 * RowDataBoundedDeltaSourceBuilder#build()} method to get the instance of a {@link DeltaSource} or
 * configure additional options using builder's API.
 */
public class RowDataContinuousDeltaSourceBuilder
    extends ContinuousDeltaSourceBuilder<RowData, RowDataContinuousDeltaSourceBuilder> {

    RowDataContinuousDeltaSourceBuilder(
            Path tablePath,
            Configuration hadoopConfiguration,
            ContinuousSnapshotSupplierFactory snapshotSupplierFactory) {
        super(tablePath, hadoopConfiguration, snapshotSupplierFactory);
    }

    //////////////////////////////////////////////////////////
    ///     We have to override methods from base class    ///
    /// to include them in javadoc generated by sbt-unidoc ///
    //////////////////////////////////////////////////////////

    /**
     * Specifies a {@link List} of column names that should be read from Delta table. If this method
     * is not used, Source will read all columns from Delta table.
     * <p>
     * Is provided List is null or contains null, empty or blank elements it will cause to throw a
     * {@code DeltaSourceValidationException} by builder after calling {@code build()} method.
     *
     * @param columnNames column names that should be read.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder columnNames(List<String> columnNames) {
        return super.columnNames(columnNames);
    }

    /**
     * Specifies an array of column names that should be read from Delta table. If this method
     * is not used, Source will read all columns from Delta table.
     * <p>
     * Is provided List is null or contains null, empty or blank elements it will cause to throw a
     * {@code DeltaSourceValidationException} by builder after calling {@code build()} method.
     *
     * @param columnNames column names that should be read.
     */
    public RowDataContinuousDeltaSourceBuilder columnNames(String... columnNames) {
        return super.columnNames(Arrays.asList(columnNames));
    }

    /**
     * Sets value of "startingVersion" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option specifies the {@link io.delta.standalone.Snapshot} version from which
     * we want to start reading the changes.
     *
     * <p>
     * This option is mutually exclusive with {@link #startingTimestamp(String)} option.
     *
     * @param startingVersion Delta {@link io.delta.standalone.Snapshot} version to start reading
     *                        changes from. The values can be string numbers like "1", "10" etc. or
     *                        keyword "latest", where in that case, changes from the latest Delta
     *                        table version will be read.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder startingVersion(String startingVersion) {
        return super.startingVersion(startingVersion);
    }

    /**
     * Sets value of "startingVersion" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option specifies the {@link io.delta.standalone.Snapshot} version from which
     * we want to start reading the changes.
     *
     * <p>
     * This option is mutually exclusive with {@link #startingTimestamp(String)} option.
     *
     * @param startingVersion Delta {@link io.delta.standalone.Snapshot} version to start reading
     *                        changes from.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder startingVersion(long startingVersion) {
        return super.startingVersion(startingVersion);
    }

    /**
     * Sets value of "startingTimestamp" option. Applicable for {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode only. This
     * option is used to read only changes from {@link io.delta.standalone.Snapshot} that was
     * generated at or before given timestamp.
     *
     * <p>
     * This option is mutually exclusive with {@link #startingVersion(String)} and {@link
     * #startingVersion(long)} option.
     *
     * @param startingTimestamp The timestamp of {@link io.delta.standalone.Snapshot} that we start
     *                          reading changes from. Supported formats are:
     *                          <ul>
     *                                <li>2022-02-24</li>
     *                                <li>2022-02-24 04:55:00</li>
     *                                <li>2022-02-24 04:55:00.001</li>
     *                                <li>2022-02-24T04:55:00</li>
     *                                <li>2022-02-24T04:55:00.001</li>
     *                                <li>2022-02-24T04:55:00.001Z</li>
     *                          </ul>
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder startingTimestamp(String startingTimestamp) {
        return super.startingTimestamp(startingTimestamp);
    }

    /**
     * Sets the value for "updateCheckIntervalMillis" option. Applicable for {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode only. This
     * option is used to specify the check interval (in milliseconds) used for periodic Delta table
     * changes checks.
     *
     * <p>
     * The default value for this option is 5000 ms.
     *
     * @param updateCheckInterval The update check internal in milliseconds.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder updateCheckIntervalMillis(
        long updateCheckInterval) {
        return super.updateCheckIntervalMillis(updateCheckInterval);
    }

    /**
     * Sets the "ignoreDeletes" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option allows processing Delta table versions containing only {@link
     * io.delta.standalone.actions.RemoveFile} actions.
     *
     * <p> If this option is set to true, Source connector will not throw an exception when
     * processing version containing only {@link io.delta.standalone.actions.RemoveFile} actions
     * regardless of {@link io.delta.standalone.actions.RemoveFile#isDataChange()} flag.
     *
     * <p>
     * The default value for these options is false.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder ignoreDeletes(boolean ignoreDeletes) {
        return super.ignoreDeletes(ignoreDeletes);
    }

    /**
     * Sets the "ignoreChanges" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option allows processing Delta table versions containing both {@link
     * io.delta.standalone.actions.RemoveFile} and {@link io.delta.standalone.actions.AddFile}
     * actions. This option subsumes {@link #ignoreDeletes} option.
     *
     * <p> If this option is set to true, Source connector will not
     * throw an exception when processing version containing combination of {@link
     * io.delta.standalone.actions.RemoveFile} and {@link io.delta.standalone.actions.AddFile}
     * actions regardless of {@link io.delta.standalone.actions.RemoveFile#isDataChange()} flag.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder ignoreChanges(boolean ignoreChanges) {
        return super.ignoreChanges(ignoreChanges);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option {@link String} value to set.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder option(String optionName, String optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option boolean value to set.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder option(String optionName, boolean optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option int value to set.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder option(String optionName, int optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option long value to set.
     */
    @Override
    public RowDataContinuousDeltaSourceBuilder option(String optionName, long optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Creates an instance of {@link DeltaSource} for a stream of {@link RowData}. Created source
     * will work in Continuous mode, actively monitoring Delta table for new changes.
     *
     * <p>
     * This method can throw {@code DeltaSourceValidationException} in case of invalid arguments
     * passed to Delta source builder.
     *
     * @return New {@link DeltaSource} instance.
     */
    @Override
    @SuppressWarnings("unchecked")
    public DeltaSource<RowData> build() {

        validate();

        // In this step, the Delta table schema discovery is made.
        // We load the snapshot corresponding to the latest/versionAsOf/timestampAsOf commit.
        // We are using this snapshot to extract the metadata and discover table's column names
        // and data types.
        SourceSchema sourceSchema = getSourceSchema();
        sourceConfiguration.addOption(INITIAL_SNAPSHOT_VERSION.key(),
            sourceSchema.getSnapshotVersion());

        DeltaBulkFormat<RowData> format = RowDataFormat.builder(
                RowType.of(sourceSchema.getColumnTypes(), sourceSchema.getColumnNames()),
                hadoopConfiguration)
            .build();

        return new DeltaSource<>(
            tablePath,
            format,
            DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER,
            hadoopConfiguration,
            sourceConfiguration
        );
    }
}
