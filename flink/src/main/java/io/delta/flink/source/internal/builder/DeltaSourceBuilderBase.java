package io.delta.flink.source.internal.builder;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceInternal;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.builder.validation.Validator;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.DeltaFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

/**
 * The base Builder class for {@link io.delta.flink.source.DeltaSource}
 */
public abstract class DeltaSourceBuilderBase<T, SELF> {

    /**
     * The provider for {@link FileSplitAssigner}.
     */
    protected static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
        LocalityAwareSplitAssigner::new;

    /**
     * The provider for {@link AddFileEnumerator}.
     */
    protected static final AddFileEnumerator.Provider<DeltaSourceSplit>
        DEFAULT_SPLITTABLE_FILE_ENUMERATOR = DeltaFileEnumerator::new;

    /**
     * Message prefix for validation exceptions.
     */
    protected static final String EXCEPTION_PREFIX = "DeltaSourceBuilder - ";

    /**
     * A placeholder object for Delta source configuration used for {@link DeltaSourceBuilderBase}
     * instance.
     */
    protected final DeltaSourceConfiguration sourceConfiguration = new DeltaSourceConfiguration();

    /**
     * A {@link Path} to Delta table that should be read by created {@link
     * io.delta.flink.source.DeltaSource}.
     */
    protected final Path tablePath;

    protected final FormatBuilder<T> formatBuilder;

    /**
     * The Hadoop's {@link Configuration} for this Source.
     */
    protected final Configuration hadoopConfiguration;

    protected DeltaSourceBuilderBase(
        Path tablePath, FormatBuilder<T> formatBuilder,
        Configuration hadoopConfiguration) {
        this.tablePath = tablePath;
        this.formatBuilder = formatBuilder;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public SELF partitions(List<String> partitions) {
        formatBuilder.partitions(partitions);
        return self();
    }

    public abstract <V extends DeltaSourceInternal<T>> V build();

    protected abstract Validator validateOptionExclusions();

    protected DeltaBulkFormat<T> validateSourceAndFormat() {
        DeltaBulkFormat<T> format = null;
        Collection<String> formatValidationMessages = Collections.emptySet();
        try {
            format = formatBuilder.build();
        } catch (DeltaSourceValidationException e) {
            formatValidationMessages = e.getValidationMessages();
        }
        validateSource(formatValidationMessages);
        return format;
    }

    protected void validateSource(Collection<String> extraValidationMessages) {
        Validator mandatoryValidator = validateMandatoryOptions();
        Validator exclusionsValidator = validateOptionExclusions();

        if (mandatoryValidator.containsMessages() || exclusionsValidator.containsMessages()) {

            List<String> validationMessages = new LinkedList<>();
            validationMessages.addAll(mandatoryValidator.getValidationMessages());
            validationMessages.addAll(exclusionsValidator.getValidationMessages());
            if (extraValidationMessages != null) {
                validationMessages.addAll(extraValidationMessages);
            }

            throw new DeltaSourceValidationException(
                SourceUtils.pathToString(tablePath),
                validationMessages);
        }
    }

    protected Validator validateMandatoryOptions() {

        return new Validator()
            // validate against null references
            .checkNotNull(tablePath, EXCEPTION_PREFIX + "missing path to Delta table.")
            .checkNotNull(hadoopConfiguration, EXCEPTION_PREFIX + "missing Hadoop configuration.");
    }

    protected String prepareOptionExclusionMessage(String... mutualExcludedOptions) {
        return String.format(
            "Used mutual excluded options for Source definition. Invalid options [%s]",
            String.join(";", mutualExcludedOptions));
    }

    protected ConfigOption<?> validateOptionName(String optionName) {
        ConfigOption<?> option = DeltaSourceOptions.VALID_SOURCE_OPTIONS.get(optionName);
        if (option == null) {
            throw DeltaSourceExceptions.invalidOptionNameException(
                SourceUtils.pathToString(tablePath), optionName);
        }
        return option;
    }

    protected void validateOptionValue(String optionName, String optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }

    protected void validateOptionValue(String optionName, int optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }

    protected void validateOptionValue(String optionName, long optionValue) {
        boolean valid = DeltaSourceOptions.getValidator(optionName).validate(optionValue);
        if (!valid) {
            throw new IllegalArgumentException(
                "Provided invalid value [" + optionValue + "] for option " + optionName);
        }
    }

    @SuppressWarnings("unchecked")
    protected SELF self() {
        return (SELF) this;
    }
}
