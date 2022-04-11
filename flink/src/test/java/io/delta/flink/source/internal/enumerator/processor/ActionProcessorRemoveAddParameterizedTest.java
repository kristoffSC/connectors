package io.delta.flink.source.internal.enumerator.processor;

import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ActionProcessorRemoveAddParameterizedTest
    extends BaseActionProcessorParameterizedTest {

    public ActionProcessorRemoveAddParameterizedTest(
        boolean ignoreChanges,
        boolean ignoreDeletes,
        ExpectedResults expectedResults) {
        super(ignoreChanges, ignoreDeletes, expectedResults);
    }

    @Parameters(name = "{index}: IgnoreChanges = [{0}], IgnoreDeletes = [{1}]")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {false, false,
                new ExpectedResults(singletonList(DeltaSourceException.class), emptyList())},
            {false, true,
                new ExpectedResults(singletonList(DeltaSourceException.class), emptyList())},
            {true, true, new ExpectedResults(singletonList(ADD_ACTION_DATA_CHANGE), emptyList())},
            {true, false, new ExpectedResults(singletonList(ADD_ACTION_DATA_CHANGE), emptyList())},
        });
    }

    @Test
    public void shouldProcessAddRemoveActionBothWithDataChangeFlag() {
        processor = new ActionProcessor(ignoreChanges, ignoreDeletes);
        boolean gotDeltaException = false;

        // GIVEN dataChangeFlag == true;
        changesToProcess = prepareChangesToProcess(
            Arrays.asList(REMOVE_ACTION_DATA_CHANGE, ADD_ACTION_DATA_CHANGE));

        // WHEN
        try {
            actualResult = processor.processActions(changesToProcess);
        } catch (DeltaSourceException e) {
            gotDeltaException = true;
        }

        // THEN
        assertResult(actualResult, expectedResults.getDataChangeResults(), gotDeltaException);
    }

    @Test
    public void shouldProcessAddRemoveActionBothWithNoDataChangeFlag() {
        processor = new ActionProcessor(ignoreChanges, ignoreDeletes);
        boolean gotDeltaException = false;

        // GIVEN dataChangeFlag == false;
        changesToProcess = prepareChangesToProcess(
            Arrays.asList(REMOVE_ACTION_NO_DATA_CHANGE, ADD_ACTION_NO_DATA_CHANGE));

        // WHEN
        try {
            actualResult = processor.processActions(changesToProcess);
        } catch (DeltaSourceException e) {
            gotDeltaException = true;
        }

        // THEN
        assertResult(actualResult, expectedResults.getNoDataChangeResults(), gotDeltaException);
    }
}
