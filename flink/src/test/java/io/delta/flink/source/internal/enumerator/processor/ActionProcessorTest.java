package io.delta.flink.source.internal.enumerator.processor;

import java.util.List;
import java.util.stream.Stream;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import io.delta.flink.source.VariableSource;
import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;

public class ActionProcessorTest extends BaseActionProcessorParameterizedTest {

    @ParameterizedTest(name = "{index}: Actions = {0}")
    @VariableSource("arguments_notIgnoreChangesAndNotIgnoreDeletes")
    public void notIgnoreChangesAndNotIgnoreDeletes(
            List<Action> inputActions,
            Object expectedResults) {

        ActionProcessor processor = new ActionProcessor(false, false);

        testProcessor(inputActions, expectedResults, processor);
    }

    @ParameterizedTest(name = "{index}: Actions = {0}")
    @VariableSource("arguments_notIgnoreChangesAndIgnoreDeletes")
    public void notIgnoreChangesAndIgnoreDeletes(
            List<Action> inputActions,
            Object expectedResults) {

        ActionProcessor processor = new ActionProcessor(false, true);

        testProcessor(inputActions, expectedResults, processor);
    }

    @ParameterizedTest(name = "{index}: Actions = {0}")
    @VariableSource("arguments_ignoreChanges")
    public void ignoreChangesAndIgnoreDeletes(List<Action> inputActions, Object expectedResults) {

        ActionProcessor processor = new ActionProcessor(true, true);

        testProcessor(inputActions, expectedResults, processor);
    }

    @ParameterizedTest(name = "{index}: Actions = {0}")
    @VariableSource("arguments_ignoreChanges")
    public void ignoreChangesAndNotIgnoreDeletes(
            List<Action> inputActions,
            Object expectedResults) {

        ActionProcessor processor = new ActionProcessor(true, false);

        testProcessor(inputActions, expectedResults, processor);
    }

    @Test
    public void shouldThrowIfInvalidActionInVersion() {

        // GIVEN
        ActionProcessor processor = new ActionProcessor(false, false);
        List<Action> actionsToProcess =
            asList(ADD_ACTION_DATA_CHANGE, REMOVE_ACTION_DATA_CHANGE, ADD_ACTION_DATA_CHANGE);
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
        ActionProcessor processor = new ActionProcessor(true, false);
        List<Action> actionsToProcess =
            asList(ADD_ACTION_DATA_CHANGE, REMOVE_ACTION_DATA_CHANGE, ADD_ACTION_DATA_CHANGE);

        processor.processActions(prepareChangesToProcess(actionsToProcess));

        // WHEN
        ChangesPerVersion<AddFile> actualResult =
            processor.processActions(prepareChangesToProcess(actionsToProcess));

        // THEN
        assertThat(actualResult.getChanges().size(), equalTo(actionsToProcess.size() - 1));
        assertThat(actualResult.getSnapshotVersion(), equalTo(SNAPSHOT_VERSION));
        assertThat(
            hasItems(new Action[]{ADD_ACTION_DATA_CHANGE, ADD_ACTION_DATA_CHANGE}).matches(
                actualResult.getChanges()),
            equalTo(true));
    }

    protected ChangesPerVersion<Action> prepareChangesToProcess(List<Action> actions) {
        return new ChangesPerVersion<>(TABLE_PATH, SNAPSHOT_VERSION, actions);
    }

    private Stream<Arguments> arguments_notIgnoreChangesAndNotIgnoreDeletes() {
        return Stream.of(
            Arguments.of(singletonList(ADD_ACTION_DATA_CHANGE),
                singletonList(ADD_ACTION_DATA_CHANGE)),
            Arguments.of(singletonList(ADD_ACTION_NO_DATA_CHANGE), emptyList()),
            Arguments.of(singletonList(REMOVE_ACTION_DATA_CHANGE), DeltaSourceException.class),
            Arguments.of(singletonList(REMOVE_ACTION_NO_DATA_CHANGE), emptyList()),

            Arguments.of(asList(ADD_ACTION_DATA_CHANGE, REMOVE_ACTION_DATA_CHANGE),
                DeltaSourceException.class),
            Arguments.of(asList(ADD_ACTION_DATA_CHANGE, REMOVE_ACTION_NO_DATA_CHANGE),
                singletonList(ADD_ACTION_DATA_CHANGE)),
            Arguments.of(asList(ADD_ACTION_NO_DATA_CHANGE, REMOVE_ACTION_DATA_CHANGE),
                DeltaSourceException.class),
            Arguments.of(asList(ADD_ACTION_NO_DATA_CHANGE, REMOVE_ACTION_NO_DATA_CHANGE),
                emptyList())
        );
    }

    private Stream<Arguments> arguments_notIgnoreChangesAndIgnoreDeletes() {
        return Stream.of(
            Arguments.of(singletonList(ADD_ACTION_DATA_CHANGE),
                singletonList(ADD_ACTION_DATA_CHANGE)),
            Arguments.of(singletonList(ADD_ACTION_NO_DATA_CHANGE), emptyList()),
            Arguments.of(singletonList(REMOVE_ACTION_DATA_CHANGE), emptyList()),
            Arguments.of(singletonList(REMOVE_ACTION_NO_DATA_CHANGE), emptyList()),

            Arguments.of(asList(ADD_ACTION_DATA_CHANGE, REMOVE_ACTION_DATA_CHANGE),
                DeltaSourceException.class),
            Arguments.of(asList(ADD_ACTION_DATA_CHANGE, REMOVE_ACTION_NO_DATA_CHANGE),
                singletonList(ADD_ACTION_DATA_CHANGE)),
            Arguments.of(asList(ADD_ACTION_NO_DATA_CHANGE, REMOVE_ACTION_DATA_CHANGE),
                emptyList()),
            Arguments.of(asList(ADD_ACTION_NO_DATA_CHANGE, REMOVE_ACTION_NO_DATA_CHANGE),
                emptyList())
        );
    }

    private Stream<Arguments> arguments_ignoreChanges() {
        return Stream.of(
            Arguments.of(singletonList(ADD_ACTION_DATA_CHANGE),
                singletonList(ADD_ACTION_DATA_CHANGE)),
            Arguments.of(singletonList(ADD_ACTION_NO_DATA_CHANGE), emptyList()),
            Arguments.of(singletonList(REMOVE_ACTION_DATA_CHANGE), emptyList()),
            Arguments.of(singletonList(REMOVE_ACTION_NO_DATA_CHANGE), emptyList()),

            Arguments.of(asList(ADD_ACTION_DATA_CHANGE, REMOVE_ACTION_DATA_CHANGE),
                singletonList(ADD_ACTION_DATA_CHANGE)),
            Arguments.of(asList(ADD_ACTION_DATA_CHANGE, REMOVE_ACTION_NO_DATA_CHANGE),
                singletonList(ADD_ACTION_DATA_CHANGE)),
            Arguments.of(asList(ADD_ACTION_NO_DATA_CHANGE, REMOVE_ACTION_DATA_CHANGE),
                emptyList()),
            Arguments.of(asList(ADD_ACTION_NO_DATA_CHANGE, REMOVE_ACTION_NO_DATA_CHANGE),
                emptyList())
        );
    }
}
