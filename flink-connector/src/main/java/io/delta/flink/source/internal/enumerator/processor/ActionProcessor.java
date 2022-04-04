package io.delta.flink.source.internal.enumerator.processor;

import java.util.ArrayList;
import java.util.List;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import static io.delta.flink.source.internal.exceptions.DeltaSourceExceptions.deltaSourceIgnoreChangesException;
import static io.delta.flink.source.internal.exceptions.DeltaSourceExceptions.deltaSourceIgnoreDeleteException;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

// TODO PR 7 add tests
public class ActionProcessor {

    private final boolean ignoreChanges;

    private final boolean ignoreDeletes;

    public ActionProcessor(boolean ignoreChanges, boolean ignoreDeletes) {
        this.ignoreChanges = ignoreChanges;
        this.ignoreDeletes = ignoreDeletes;
    }

    public ChangesPerVersion<AddFile> processActions(ChangesPerVersion<Action> changesToProcess) {

        List<AddFile> addFiles = new ArrayList<>(changesToProcess.size());
        boolean seenAddFile = false;
        boolean seenRemovedFile = false;

        for (Action action : changesToProcess.getChanges()) {
            DeltaActions deltaActions = DeltaActions.instanceFrom(action.getClass());
            switch (deltaActions) {
                case ADD:
                    if (((AddFile) action).isDataChange()) {
                        seenAddFile = true;
                        addFiles.add((AddFile) action);
                    }
                    break;
                case REMOVE:
                    if (((RemoveFile) action).isDataChange()) {
                        seenRemovedFile = true;
                    }
                    break;
                case METADATA:
                    // TODO implement schema compatibility check similar as it is done in
                    //  https://github.com/delta-io/delta/blob/0d07d094ccd520c1adbe45dde4804c754c0a4baa/core/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSource.scala#L422
                default:
                    // Do nothing.
                    break;
            }
            actionsSanityCheck(seenAddFile, seenRemovedFile, changesToProcess);
        }

        return new ChangesPerVersion<>(
            changesToProcess.getDeltaTablePath(), changesToProcess.getSnapshotVersion(), addFiles);
    }

    private void actionsSanityCheck(boolean seenFileAdd, boolean seenRemovedFile,
        ChangesPerVersion<Action> changesToProcess) {
        if (seenRemovedFile) {
            if (seenFileAdd && !ignoreChanges) {
                deltaSourceIgnoreChangesException(changesToProcess.getDeltaTablePath(),
                    changesToProcess.getSnapshotVersion());
            } else if (!seenFileAdd && !ignoreDeletes) {
                deltaSourceIgnoreDeleteException(changesToProcess.getDeltaTablePath(),
                    changesToProcess.getSnapshotVersion());
            }
        }
    }
}