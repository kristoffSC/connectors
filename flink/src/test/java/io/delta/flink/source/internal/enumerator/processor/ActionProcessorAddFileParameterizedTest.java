package io.delta.flink.source.internal.enumerator.processor;

import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ActionProcessorAddFileParameterizedTest extends BaseActionProcessorParameterizedTest {

    public ActionProcessorAddFileParameterizedTest(
        boolean ignoreChanges,
        boolean ignoreDeletes,
        ExpectedResults expectedResults) {
        super(ignoreChanges, ignoreDeletes, expectedResults);
    }

    @Parameters(name = "{index}: IgnoreChanges = [{0}], IgnoreDeletes = [{1}]")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {false, false, new ExpectedResults(singletonList(ADD_ACTION_DATA_CHANGE), emptyList())},
            {false, true, new ExpectedResults(singletonList(ADD_ACTION_DATA_CHANGE), emptyList())},
            {true, true, new ExpectedResults(singletonList(ADD_ACTION_DATA_CHANGE), emptyList())},
            {true, false, new ExpectedResults(singletonList(ADD_ACTION_DATA_CHANGE), emptyList())},
        });
    }

    @Test
    public void shouldProcessAddActionWithDataChangeFlag() {
        processor = new ActionProcessor(ignoreChanges, ignoreDeletes);

        // GIVEN dataChangeFlag == true;
        changesToProcess = prepareChangesToProcess(singletonList(ADD_ACTION_DATA_CHANGE));

        // WHEN
        actualResult = processor.processActions(changesToProcess);

        // THEN
        assertThat(actualResult.getChanges().size(),
            equalTo(expectedResults.getDataChangeResults().size()));
        assertTrue(
            hasItems(expectedResults.getDataChangeResults().toArray()).matches(
                actualResult.getChanges()));
    }

    @Test
    public void shouldProcessAddActionWithNoDataChangeFlag() {
        processor = new ActionProcessor(ignoreChanges, ignoreDeletes);

        // GIVEN dataChangeFlag == false;
        changesToProcess = prepareChangesToProcess(singletonList(ADD_ACTION_NO_DATA_CHANGE));

        // WHEN
        actualResult = processor.processActions(changesToProcess);

        // THEN
        assertThat(actualResult.getChanges().size(),
            equalTo(expectedResults.getNoDataChangeResults().size()));
        assertTrue(hasItems(expectedResults.getNoDataChangeResults().toArray()).matches(
            actualResult.getChanges()));
    }
}
