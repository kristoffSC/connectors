package io.delta.flink.source.internal.enumerator;

import java.util.Collections;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
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

    }

    @Test
    public void shouldUseVersionAsOfSnapshot() {

    }

    @Test
    public void shouldUseTimestampAsOfSnapshot() {

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

    @Override
    protected DeltaSourceSplitEnumerator createEnumerator() {
        when(splitAssignerProvider.create(Mockito.any())).thenReturn(splitAssigner);
        when(fileEnumeratorProvider.create()).thenReturn(fileEnumerator);

        ContinuousSplitEnumeratorProvider provider =
            new ContinuousSplitEnumeratorProvider(splitAssignerProvider, fileEnumeratorProvider);

        return (DeltaSourceSplitEnumerator) provider.createInitialStateEnumerator(deltaTablePath,
            DeltaSinkTestUtils.getHadoopConf(), enumContext, sourceConfiguration);
    }

    @Override
    protected DeltaSourceSplitEnumerator createEnumerator(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint) {
        return null;
    }
}

