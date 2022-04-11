package io.delta.flink.source.internal.enumerator.processor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public class ActionProcessorTest {

    private static final long SNAPSHOT_VERSION = 10;

    private static final int SIZE = 100;

    private static final String TABLE_PATH = "s3://some/path/";

    private static final Map<String, String> PARTITIONS = Collections.emptyMap();

    private static final Map<String, String> TAGS = Collections.emptyMap();

    private static final String PATH = TABLE_PATH + "file.parquet";

    private static final AddFile ADD_FILE =
        new AddFile(PATH, PARTITIONS, SIZE, System.currentTimeMillis(), true, "", TAGS);

    private static final RemoveFile REMOVE_FILE = ADD_FILE.remove(true);

    private ActionProcessor processor;

    @Before
    public void setUp() {
        // ignore deletes and ignore changes set to false
        processor = new ActionProcessor(false, false);
    }

    @Test
    public void shouldProcessAllAddActions() {

        // GIVEN
        List<Action> actionsToProcess = Arrays.asList(ADD_FILE, ADD_FILE, ADD_FILE);

        // WHEN
        ChangesPerVersion<AddFile> actualResult =
            processor.processActions(prepareChangesToProcess(actionsToProcess));

        // THEN
        assertThat(actualResult.getChanges().size(), equalTo(actionsToProcess.size()));
        assertThat(actualResult.getSnapshotVersion(), equalTo(SNAPSHOT_VERSION));
        assertThat(
            hasItems(actionsToProcess.toArray()).matches(actualResult.getChanges()),
            equalTo(true));
    }

    @Test
    public void shouldThrowIfInvalidActionInVersion() {

        // GIVEN
        List<Action> actionsToProcess = Arrays.asList(ADD_FILE, REMOVE_FILE, ADD_FILE);
        DeltaSourceException caughtException = null;

        // WHEN
        try {
            processor.processActions(prepareChangesToProcess(actionsToProcess));
        } catch (DeltaSourceException e) {
            caughtException = e;
        }

        // THEN
        assertThat(caughtException, notNullValue());
        assertThat(caughtException.getSnapshotVersion().orElse(null), equalTo(SNAPSHOT_VERSION));
        assertThat(caughtException.getTablePath(), equalTo(TABLE_PATH));
    }

    @Test
    public void shouldFilterOutRemoveIfIgnoreChangesFlag() {
        // GIVEN
        processor = new ActionProcessor(true, false);
        List<Action> actionsToProcess = Arrays.asList(ADD_FILE, REMOVE_FILE, ADD_FILE);

        processor.processActions(prepareChangesToProcess(actionsToProcess));

        // WHEN
        ChangesPerVersion<AddFile> actualResult =
            processor.processActions(prepareChangesToProcess(actionsToProcess));

        // THEN
        assertThat(actualResult.getChanges().size(), equalTo(actionsToProcess.size() - 1));
        assertThat(actualResult.getSnapshotVersion(), equalTo(SNAPSHOT_VERSION));
        assertThat(
            hasItems(new Action[] {ADD_FILE, ADD_FILE}).matches(actualResult.getChanges()),
            equalTo(true));
    }

    protected ChangesPerVersion<Action> prepareChangesToProcess(List<Action> actions) {
        return new ChangesPerVersion<>(TABLE_PATH, SNAPSHOT_VERSION, actions);
    }

}
