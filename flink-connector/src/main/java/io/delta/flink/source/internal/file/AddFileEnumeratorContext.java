package io.delta.flink.source.internal.file;

import java.util.List;

import io.delta.standalone.actions.AddFile;

public class AddFileEnumeratorContext {

    private final String tablePath;
    private final List<AddFile> addFiles;

    public AddFileEnumeratorContext(String tablePath, List<AddFile> addFiles) {
        this.tablePath = tablePath;
        this.addFiles = addFiles;
    }

    public String getTablePath() {
        return tablePath;
    }

    public List<AddFile> getAddFiles() {
        return addFiles;
    }
}
