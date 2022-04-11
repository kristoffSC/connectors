package io.delta.flink.source.internal.enumerator.processor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public abstract class BaseActionProcessorParameterizedTest {

    protected static final int SIZE = 100;

    protected static final long SNAPSHOT_VERSION = 10;

    protected static final String TABLE_PATH = "s3://some/path/";

    protected static final Map<String, String> PARTITIONS = Collections.emptyMap();

    protected static final Map<String, String> TAGS = Collections.emptyMap();

    protected static final String PATH = TABLE_PATH + "file.parquet";

    protected static final AddFile ADD_ACTION_DATA_CHANGE =
        new AddFile(PATH, PARTITIONS, SIZE, System.currentTimeMillis(), true, "", TAGS);

    protected static final RemoveFile REMOVE_ACTION_DATA_CHANGE =
        ADD_ACTION_DATA_CHANGE.remove(true);

    protected static final AddFile ADD_ACTION_NO_DATA_CHANGE =
        new AddFile(PATH, PARTITIONS, 100, System.currentTimeMillis(), false, "", TAGS);

    protected static final RemoveFile REMOVE_ACTION_NO_DATA_CHANGE =
        ADD_ACTION_NO_DATA_CHANGE.remove(false);

    protected final boolean ignoreChanges;

    protected final boolean ignoreDeletes;

    protected final ExpectedResults expectedResults;

    protected ActionProcessor processor;

    protected ChangesPerVersion<Action> changesToProcess;

    protected ChangesPerVersion<AddFile> actualResult;

    public BaseActionProcessorParameterizedTest(
        boolean ignoreChanges,
        boolean ignoreDeletes,
        ExpectedResults expectedResults) {
        this.ignoreChanges = ignoreChanges;
        this.ignoreDeletes = ignoreDeletes;
        this.expectedResults = expectedResults;
    }

    protected void assertResult(
        ChangesPerVersion<AddFile> actualResult,
        List<Object> expectedResults,
        boolean gotDeltaException) {

        if (expectedResults.size() == 1
            && expectedResults.get(0).equals(DeltaSourceException.class)) {
            assertThat(gotDeltaException, equalTo(true));
        } else {
            assertThat(actualResult.getChanges().size(), equalTo(expectedResults.size()));
            assertThat(
                hasItems(expectedResults.toArray()).matches(actualResult.getChanges()),
                equalTo(true));
        }
    }

    protected ChangesPerVersion<Action> prepareChangesToProcess(List<Action> actions) {
        return new ChangesPerVersion<>(TABLE_PATH, SNAPSHOT_VERSION, actions);
    }

    public static class ExpectedResults {

        private final List<Object> dataChangeResults;

        private final List<Object> noDataChangeResults;

        public ExpectedResults(List<Object> dataChangeResults,
            List<Object> noDataChangeResults) {
            this.dataChangeResults = dataChangeResults;
            this.noDataChangeResults = noDataChangeResults;
        }

        public List<Object> getDataChangeResults() {
            return dataChangeResults;
        }

        public List<Object> getNoDataChangeResults() {
            return noDataChangeResults;
        }
    }
}
