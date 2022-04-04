package io.delta.flink.source.internal.enumerator;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import io.delta.flink.source.internal.enumerator.monitor.TableMonitor;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitorResult;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContinuousDeltaSourceSplitEnumeratorTest extends DeltaSourceSplitEnumeratorTestBase {

    private ContinuousDeltaSourceSplitEnumerator enumerator;

    private ContinuousSplitEnumeratorProvider provider;

    @Captor
    private ArgumentCaptor<TableMonitor> tableMonitorArgumentCaptor;

    @Before
    public void setUp() {
        super.setUp();

        when(splitAssignerProvider.create(Mockito.any())).thenReturn(splitAssigner);
        when(fileEnumeratorProvider.create()).thenReturn(fileEnumerator);

        provider =
            new ContinuousSplitEnumeratorProvider(splitAssignerProvider, fileEnumeratorProvider);
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void shouldNotReadInitialSnapshotWhenMonitoringForChanges() {

        long snapshotVersion = 10;

        Mockito.doAnswer(invocation -> {
            TableMonitor tableMonitor = invocation.getArgument(0, TableMonitor.class);
            assertThat(tableMonitor.getMonitorVersion(), equalTo(snapshotVersion));
            tableMonitor.call();
            return new TableMonitorResult(snapshotVersion, Collections.emptyList());
        }).when(enumContext)
            .callAsync(any(Callable.class), any(BiConsumer.class), anyLong(), anyLong());

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint =
            DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(deltaTablePath, snapshotVersion,
                true,
                Collections.emptyList(), Collections.emptyList());

        enumerator = setUpEnumeratorFromCheckpoint(checkpoint);
        enumerator.start();

        // verify that we did not create any snapshot, we only need to get changes from deltaLog.
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // verify that we try to get changes from Delta Log.
        verify(enumContext).callAsync(any(Callable.class), any(BiConsumer.class), anyLong(),
            anyLong());

        // TODO PR 7 - uncomment after implementing TableMonitor::call
        //verify(deltaLog).getChanges(snapshotVersion, true);
    }

    @Test
    public void shouldReadInitialSnapshotWhenNotMonitoringForChanges() {
        long snapshotVersion = 10;

        when(deltaLog.getSnapshotForVersionAsOf(snapshotVersion)).thenReturn(checkpointedSnapshot);
        when(checkpointedSnapshot.getVersion()).thenReturn(snapshotVersion);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint =
            DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(deltaTablePath, snapshotVersion,
                false,
                Collections.emptyList(), Collections.emptyList());

        enumerator = setUpEnumeratorFromCheckpoint(checkpoint);
        enumerator.start();

        // verify that snapshot was created using version from checkpoint and not head or timestamp.
        verify(deltaLog).getSnapshotForVersionAsOf(snapshotVersion);
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // verify that we tried to read initial snapshot content.
        verify(checkpointedSnapshot).getAllFiles();

        // verify TableMonitor starting version
        verify(enumContext).callAsync(tableMonitorArgumentCaptor.capture(), any(),
            anyLong(), anyLong());
        assertThat(tableMonitorArgumentCaptor.getValue().getMonitorVersion(),
            equalTo(snapshotVersion + 1));
    }

    @Test
    public void shouldNotSignalNoMoreSplitsIfNone() {
        int subtaskId = 1;
        enumerator = setUpEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        enumerator.handleSplitRequest(subtaskId, "testHost");

        verify(enumerator).handleNoMoreSplits(subtaskId);
        verify(enumContext, never()).signalNoMoreSplits(subtaskId);
    }

    // TODO Add in PR 7
    //@Test
    public void shouldOnlyReadChangesWhenStartingVersionOption() {

    }

    // TODO Add in PR 7
    //@Test
    public void shouldOnlyReadChangesWhenStartingTimestampOption() {

    }

    @Override
    protected SplitEnumeratorProvider getProvider() {
        return this.provider;
    }

}
