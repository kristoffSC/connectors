package io.delta.flink.source.internal.enumerator.monitor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;

@RunWith(MockitoJUnitRunner.class)
public class TableMonitorTest {

    private static final int SIZE = 100;

    private static final String TABLE_PATH = "s3://some/path/";

    private static final Map<String, String> PARTITIONS = Collections.emptyMap();

    private static final Map<String, String> TAGS = Collections.emptyMap();

    private static final String PATH = TABLE_PATH + "file.parquet";

    private static final AddFile ADD_FILE =
        new AddFile(PATH, PARTITIONS, SIZE, System.currentTimeMillis(), true, "", TAGS);

    private static final long MONITOR_VERSION = 10;

    private static final long MAX_DURATION_MILLIS = 4000;

    private static final ExecutorService WORKER_EXECUTOR = Executors.newSingleThreadExecutor();
    @Mock
    private DeltaLog deltaLog;

    private TableMonitor tableMonitor;

    @Before
    public void setUp() {
        when(deltaLog.getPath()).thenReturn(new Path(TABLE_PATH));
    }

    @Test
    public void shouldDiscoverVersions() throws Exception {

        // GIVEN
        List<VersionLog> versions =
            Arrays.asList(new VersionLog(MONITOR_VERSION, Collections.singletonList(ADD_FILE)),
                new VersionLog(MONITOR_VERSION + 1, Collections.singletonList(ADD_FILE)),
                new VersionLog(MONITOR_VERSION + 2, Collections.singletonList(ADD_FILE)),
                new VersionLog(MONITOR_VERSION + 3, Collections.singletonList(ADD_FILE)));

        when(deltaLog.getChanges(MONITOR_VERSION, true)).thenReturn(versions.iterator());

        // WHEN
        tableMonitor = new TableMonitor(deltaLog, MONITOR_VERSION, MAX_DURATION_MILLIS);
        Future<TableMonitorResult> future = WORKER_EXECUTOR.submit(tableMonitor);
        // Timeout on get to prevent waiting forever and hanging the build.
        TableMonitorResult result = future.get(MAX_DURATION_MILLIS * 2, TimeUnit.MILLISECONDS);

        // THEN
        List<ChangesPerVersion<Action>> changes = result.getChanges();
        assertThat(changes.size(), equalTo(versions.size()));
        assertThat(changes.get(changes.size() - 1).getSnapshotVersion(),
            equalTo(MONITOR_VERSION + 3));

        assertThat("Table next version used for monitoring should be last discovered version + 1",
            tableMonitor.getMonitorVersion(),
            equalTo(versions.get(versions.size() - 1).getVersion() + 1));
    }

    @Test
    public void shouldHandleNoNewChanges() throws Exception {

        // GIVEN
        when(deltaLog.getChanges(MONITOR_VERSION, true)).thenReturn(Collections.emptyIterator());

        // WHEN
        tableMonitor = new TableMonitor(deltaLog, MONITOR_VERSION, MAX_DURATION_MILLIS);
        Future<TableMonitorResult> future = WORKER_EXECUTOR.submit(tableMonitor);
        // Timeout on get to prevent waiting forever and hanging the build.
        TableMonitorResult result = future.get(MAX_DURATION_MILLIS * 2, TimeUnit.MILLISECONDS);

        // THEN
        List<ChangesPerVersion<Action>> changes = result.getChanges();
        assertThat(changes.size(), equalTo(0));

        assertThat("The next monitoring version should not be updated if no changes were found.",
            tableMonitor.getMonitorVersion(), equalTo(MONITOR_VERSION));
    }

    @Test
    public void shouldReturnAfterExceedingMaxDurationLimit() throws Exception {
        // GIVEN
        VersionLog longTakingVersion = mock(VersionLog.class);

        // mock a long operation on VersionLog object
        when(longTakingVersion.getActions()).then((Answer<List<Action>>) invocation -> {
            Thread.sleep(MAX_DURATION_MILLIS + 1000);
            return Collections.singletonList(ADD_FILE);
        });
        when(longTakingVersion.getVersion()).thenReturn(MONITOR_VERSION + 2);

        List<VersionLog> versions =
            Arrays.asList(new VersionLog(MONITOR_VERSION, Collections.singletonList(ADD_FILE)),
                new VersionLog(MONITOR_VERSION + 1, Collections.singletonList(ADD_FILE)),
                longTakingVersion,
                new VersionLog(MONITOR_VERSION + 3, Collections.singletonList(ADD_FILE)));

        when(deltaLog.getChanges(MONITOR_VERSION, true)).thenReturn(versions.iterator());

        // WHEN
        tableMonitor = new TableMonitor(deltaLog, MONITOR_VERSION, MAX_DURATION_MILLIS);
        Future<TableMonitorResult> future = WORKER_EXECUTOR.submit(tableMonitor);
        // Timeout on get to prevent waiting forever and hanging the build.
        TableMonitorResult result = future.get(MAX_DURATION_MILLIS * 2, TimeUnit.MILLISECONDS);

        // THEN
        List<ChangesPerVersion<Action>> changes = result.getChanges();
        assertThat(changes.size(), equalTo(versions.size() - 1));
        assertThat("The last discovered, returned version should be the long taking version.",
            changes.get(changes.size() - 1).getSnapshotVersion(),
            equalTo(longTakingVersion.getVersion()));

        assertThat("Table next version used for monitoring should be last discovered version + 1",
            tableMonitor.getMonitorVersion(), equalTo(longTakingVersion.getVersion() + 1));
    }


}
