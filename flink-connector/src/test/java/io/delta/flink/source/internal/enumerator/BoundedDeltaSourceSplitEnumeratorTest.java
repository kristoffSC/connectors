package io.delta.flink.source.internal.enumerator;

import java.util.Collections;
import java.util.List;
import static java.util.Collections.emptyList;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static io.delta.flink.source.internal.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.internal.DeltaSourceOptions.VERSION_AS_OF;
import static io.delta.flink.source.internal.enumerator.SourceSplitEnumeratorTestUtils.mockSplits;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.Snapshot;

@RunWith(MockitoJUnitRunner.class)
public class BoundedDeltaSourceSplitEnumeratorTest extends DeltaSourceSplitEnumeratorTestBase {

    @Mock
    private Snapshot versionAsOfSnapshot;

    @Mock
    private Snapshot timestampAsOfSnapshot;

    private BoundedDeltaSourceSplitEnumerator enumerator;

    @Before
    public void setUp() {
        super.setUp();
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void shouldUseCheckpointSnapshot() {
        int checkpointedSnapshotVersion = 10;

        when(deltaLog.getSnapshotForVersionAsOf(checkpointedSnapshotVersion)).thenReturn(
            checkpointedSnapshot);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>
            checkpoint = DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(deltaTablePath,
            checkpointedSnapshotVersion, checkpointedSnapshotVersion, emptyList(), emptyList());

        enumerator = BoundedDeltaSourceSplitEnumerator.createForCheckpoint(
            checkpoint, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration);

        assertThat(enumerator.getInitialSnapshot(), equalTo(checkpointedSnapshot));
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).snapshot();
        verify(deltaLog).getSnapshotForVersionAsOf(checkpointedSnapshotVersion);
    }

    @Test
    public void shouldUseVersionAsOfSnapshot() {
        long versionAsOf = 77;

        sourceConfiguration.addOption(VERSION_AS_OF.key(), versionAsOf);
        when(deltaLog.getSnapshotForVersionAsOf(versionAsOf)).thenReturn(versionAsOfSnapshot);

        enumerator = BoundedDeltaSourceSplitEnumerator.create(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration);

        List<DeltaSourceSplit> mockSplits = mockSplits();
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenReturn(mockSplits);

        enumerator.start();

        assertThat(enumerator.getInitialSnapshot(), equalTo(versionAsOfSnapshot));
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).snapshot();
        verify(deltaLog).getSnapshotForVersionAsOf(versionAsOf);
    }

    @Test
    public void shouldUseTimestampAsOfSnapshot() {
        long timestampAsOf = System.currentTimeMillis();

        sourceConfiguration.addOption(TIMESTAMP_AS_OF.key(), timestampAsOf);
        when(deltaLog.getSnapshotForTimestampAsOf(timestampAsOf)).thenReturn(timestampAsOfSnapshot);

        enumerator = BoundedDeltaSourceSplitEnumerator.create(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration);

        List<DeltaSourceSplit> mockSplits = mockSplits();
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenReturn(mockSplits);

        enumerator.start();

        assertThat(enumerator.getInitialSnapshot(), equalTo(timestampAsOfSnapshot));
        verify(deltaLog).getSnapshotForTimestampAsOf(timestampAsOf);
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
    }

    @Test
    public void shouldSignalNoMoreSplitsIfNone() {
        int subtaskId = 1;
        enumerator = setupEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        enumerator.handleSplitRequest(subtaskId, "testHost");

        verify(enumContext).signalNoMoreSplits(subtaskId);
    }

    private BoundedDeltaSourceSplitEnumerator setupEnumeratorWithHeadSnapshot() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        return BoundedDeltaSourceSplitEnumerator.create(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceConfiguration);
    }

    @Override
    protected DeltaSourceSplitEnumerator createEnumerator() {
        return BoundedDeltaSourceSplitEnumerator.create(
            deltaTablePath, fileEnumerator, splitAssigner,
            DeltaSinkTestUtils.getHadoopConf(), enumContext, sourceConfiguration);
    }
}

