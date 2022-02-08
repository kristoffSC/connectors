package io.delta.flink.source.internal.file;

import java.util.List;

import io.delta.standalone.actions.AddFile;

/**
 * This class provides a context and input for {@link AddFileEnumerator} needed to convert {@link
 * AddFile} to Splits.
 */
public class AddFileEnumeratorContext {

    /**
     * Path to Delta Table for which this context is created.
     */
    private final String tablePath;

    /**
     * A list of {@link AddFile} that should be converted to Splits in scope of this context.
     */
    private final List<AddFile> addFiles;

    /**
     * Creates AddFileEnumeratorContext for given {@code tablePath} and {@code addFiles} list.
     *
     * @param tablePath A path for Delta Table for witch this context was created.
     * @param addFiles  A list of {@link AddFile} that should be converted to Splits and are coming
     *                  from {@code tablePath}.
     */
    public AddFileEnumeratorContext(String tablePath, List<AddFile> addFiles) {
        this.tablePath = tablePath;
        this.addFiles = addFiles;
    }

    /**
     * @return Path to Delta Table for which this context is created.
     */
    public String getTablePath() {
        return tablePath;
    }

    /**
     * @return A list of {@link AddFile} that should be converted to Splits in scope of this
     * context.
     */
    public List<AddFile> getAddFiles() {
        return addFiles;
    }
}
