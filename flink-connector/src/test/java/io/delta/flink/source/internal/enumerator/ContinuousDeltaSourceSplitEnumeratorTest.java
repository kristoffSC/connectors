package io.delta.flink.source.internal.enumerator;

import java.util.Collections;
import static java.util.Collections.emptyList;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContinuousDeltaSourceSplitEnumeratorTest extends DeltaSourceSplitEnumeratorTestBase {

    private ContinuousDeltaSourceSplitEnumerator enumerator;

    @Before
    public void setUp() {
        super.setUp();
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void shouldNotReadInitialSnapshotIfRecoveryFromNewVersion() {
        long initialSnapshotVersion = 1;
        long currentSnapshotVersion = 10;

        when(deltaLog.getSnapshotForVersionAsOf(initialSnapshotVersion)).thenReturn(headSnapshot);
        when(headSnapshot.getVersion()).thenReturn(initialSnapshotVersion);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>
            checkpoint = DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(deltaTablePath,
            initialSnapshotVersion, currentSnapshotVersion, emptyList(), emptyList());

        enumerator = spy(ContinuousDeltaSourceSplitEnumerator.create(
            checkpoint, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration));

        enumerator.start();

        assertThat(enumerator.getSnapshot(), equalTo(headSnapshot));

        verify(enumerator, never()).readTableInitialContent();
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // The snapshot is initialized from initial version. The snapshot is used only for initial
        // table load and not for monitoring for changes.
        verify(deltaLog).getSnapshotForVersionAsOf(initialSnapshotVersion);
    }

    @Test
    public void shouldReadInitialSnapshotAgainIfRecoveryFromInitialVersion() {
        long initialSnapshotVersion = 1;
        long currentSnapshotVersion = 1;

        when(deltaLog.getSnapshotForVersionAsOf(initialSnapshotVersion)).thenReturn(headSnapshot);
        when(headSnapshot.getVersion()).thenReturn(initialSnapshotVersion);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>
            checkpoint = DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(deltaTablePath,
            initialSnapshotVersion, currentSnapshotVersion, emptyList(), emptyList());

        enumerator = spy(ContinuousDeltaSourceSplitEnumerator.createForCheckpoint(
            checkpoint, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration));

        enumerator.start();

        assertThat(enumerator.getSnapshot(), equalTo(headSnapshot));

        verify(enumerator).readTableInitialContent();
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // The snapshot is initialized from initial version. The snapshot is used only for initial
        // table load and not for monitoring for changes.
        verify(deltaLog).getSnapshotForVersionAsOf(initialSnapshotVersion);
    }

    // TODO PR 7 Add tests for startingVersion and startingTimestamp

    @Test
    public void shouldNotSignalNoMoreSplitsIfNone() {
        int subtaskId = 1;
        enumerator = setUpSpyEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        enumerator.handleSplitRequest(subtaskId, "testHost");

        verify(enumerator).handleNoMoreSplits(subtaskId);
        verify(enumContext, never()).signalNoMoreSplits(subtaskId);
    }

    @Test
    public void shouldOnlyReadChangesWhenStartingVersionOption() {
        sourceConfiguration.addOption(DeltaSourceOptions.STARTING_VERSION.key(), 10);
        enumerator = setUpSpyEnumeratorWithHeadSnapshot();

        enumerator.start();

        assertThat(enumerator.getSnapshot(), equalTo(headSnapshot));
        verify(enumerator, never()).readTableInitialContent();
    }

    @Test
    public void shouldOnlyReadChangesWhenStartingTimestampOption() {
        sourceConfiguration.addOption(DeltaSourceOptions.STARTING_TIMESTAMP.key(),
            System.currentTimeMillis());
        enumerator = setUpSpyEnumeratorWithHeadSnapshot();

        enumerator.start();

        assertThat(enumerator.getSnapshot(), equalTo(headSnapshot));
        verify(enumerator, never()).readTableInitialContent();
    }

    /**
     * In this test check that we do not read the table state after recovery from a checkpoint. The
     * table content should be read only for the initial Source setup or when recovering without a
     * checkpoint. Otherwise, Source should read only changes from provided checkpointVersion and no
     * entire table content for this version.
     */
    @Test
    public void shouldNotReadTableContentIfNotOnInitialVersion() {
        long initialSnapshotVersion = 1;
        long currentSnapshotVersion = 10;

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>
            checkpoint = DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(deltaTablePath,
            initialSnapshotVersion, currentSnapshotVersion, emptyList(), emptyList());

        when(headSnapshot.getVersion()).thenReturn(initialSnapshotVersion);
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        // Enumerator that is first time created thus it should read initial table content from
        // initial snapshot version.
        enumerator = ContinuousDeltaSourceSplitEnumerator.create(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration);

        enumerator.start();
        verify(splitAssigner).addSplits(any());

        // Enumerator that is recreated from Snapshot hence it has initial snapshot version and
        // checkpoint snapshot version.
        when(deltaLog.getSnapshotForVersionAsOf(initialSnapshotVersion)).thenReturn(headSnapshot);
        enumerator = ContinuousDeltaSourceSplitEnumerator.create(
            checkpoint, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration);

        Mockito.reset(splitAssigner);
        enumerator.start();
        verify(splitAssigner, never()).addSplits(any());
    }

    @Override
    protected DeltaSourceSplitEnumerator createEnumerator() {
        return ContinuousDeltaSourceSplitEnumerator.create(
            deltaTablePath, fileEnumerator, splitAssigner,
            DeltaSinkTestUtils.getHadoopConf(), enumContext, sourceConfiguration);
    }

    private ContinuousDeltaSourceSplitEnumerator setUpSpyEnumeratorWithHeadSnapshot() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        return spy(ContinuousDeltaSourceSplitEnumerator.create(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration));
    }
}
