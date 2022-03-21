package io.delta.flink.source.internal.enumerator;

import java.util.Collections;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import static org.mockito.Mockito.never;
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

    }

    @Test
    public void shouldReadInitialSnapshotAgainIfRecoveryFromInitialVersion() {

    }

    // TODO PR 7 Add tests for startingVersion and startingTimestamp

    @Test
    public void shouldNotSignalNoMoreSplitsIfNone() {
        int subtaskId = 1;
        enumerator = setupEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        enumerator.handleSplitRequest(subtaskId, "testHost");

        verify(enumerator).handleNoMoreSplits(subtaskId);
        verify(enumContext, never()).signalNoMoreSplits(subtaskId);
    }

    @Test
    public void shouldOnlyReadChangesWhenStartingVersionOption() {

    }

    @Test
    public void shouldOnlyReadChangesWhenStartingTimestampOption() {

    }

    /**
     * In this test check that we do not read the table state after recovery from a checkpoint. The
     * table content should be read only for the initial Source setup or when recovering without a
     * checkpoint. Otherwise, Source should read only changes from provided checkpointVersion and no
     * entire table content for this version.
     */
    @Test
    public void shouldNotReadTableContentIfNotOnInitialVersion() {

    }

    @Override
    protected DeltaSourceSplitEnumerator createEnumerator() {

        when(splitAssignerProvider.create(Mockito.any())).thenReturn(splitAssigner);
        when(fileEnumeratorProvider.create()).thenReturn(fileEnumerator);

        BoundedSplitEnumeratorProvider provider =
            new BoundedSplitEnumeratorProvider(splitAssignerProvider, fileEnumeratorProvider);

        return (DeltaSourceSplitEnumerator) provider.createInitialStateEnumerator(deltaTablePath,
            DeltaSinkTestUtils.getHadoopConf(), enumContext, sourceConfiguration);

    }

    @Override
    protected DeltaSourceSplitEnumerator createEnumerator(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint) {
        return null;
    }
}
