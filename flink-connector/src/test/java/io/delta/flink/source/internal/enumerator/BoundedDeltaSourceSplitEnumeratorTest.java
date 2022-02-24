package io.delta.flink.source.internal.enumerator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import static io.delta.flink.source.internal.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.internal.DeltaSourceOptions.VERSION_AS_OF;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

@RunWith(MockitoJUnitRunner.class)
public class BoundedDeltaSourceSplitEnumeratorTest {

    private static final String TEST_PATH = "/some/path/file.txt";

    @Mock
    private Path deltaTablePath;

    @Mock
    private AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    @Mock
    private FileSplitAssigner splitAssigner;

    @Mock
    private SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    @Mock
    private DeltaLog deltaLog;

    @Mock
    private Snapshot headSnapshot;

    @Mock
    private Snapshot checkpointedSnapshot;

    @Mock
    private Snapshot versionAsOfSnapshot;

    @Mock
    private Snapshot timestampAsOfSnapshot;

    @Mock
    private ReaderInfo readerInfo;

    @Mock
    private DeltaSourceSplit split;

    @Captor
    private ArgumentCaptor<List<FileSourceSplit>> splitsCaptor;

    private BoundedDeltaSourceSplitEnumerator enumerator;

    private MockedStatic<SourceUtils> sourceUtils;

    private MockedStatic<DeltaLog> deltaLogStatic;

    private DeltaSourceConfiguration sourceOptions;

    @Before
    public void setUp() {
        sourceOptions = new DeltaSourceConfiguration();
        deltaLogStatic = Mockito.mockStatic(DeltaLog.class);
        deltaLogStatic.when(() -> DeltaLog.forTable(any(Configuration.class), anyString()))
            .thenReturn(this.deltaLog);

        sourceUtils = Mockito.mockStatic(SourceUtils.class);
        sourceUtils.when(() -> SourceUtils.pathToString(deltaTablePath))
            .thenReturn(TEST_PATH);
    }

    @After
    public void after() {
        sourceUtils.close();
        deltaLogStatic.close();
    }

    @Test
    public void shouldUseHeadSnapshot() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        enumerator = new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceOptions);

        assertThat(enumerator.getSnapshot(), equalTo(headSnapshot));
        verify(deltaLog).snapshot();
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
    }

    @Test
    public void shouldUseCheckpointSnapshot() {
        int checkpointedSnapshotVersion = 10;

        when(deltaLog.getSnapshotForVersionAsOf(checkpointedSnapshotVersion)).thenReturn(
            checkpointedSnapshot);

        enumerator = new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceOptions, checkpointedSnapshotVersion, Collections.emptyList());

        assertThat(enumerator.getSnapshot(), equalTo(checkpointedSnapshot));
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).snapshot();
        verify(deltaLog).getSnapshotForVersionAsOf(10);
    }

    @Test
    public void shouldUseVersionAsOfSnapshot() {
        long versionAsOf = 77;

        sourceOptions.addOption(VERSION_AS_OF.key(), versionAsOf);
        when(deltaLog.getSnapshotForVersionAsOf(versionAsOf)).thenReturn(versionAsOfSnapshot);

        enumerator = new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceOptions);

        List<DeltaSourceSplit> mockSplits = mockSplits();
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenReturn(mockSplits);

        enumerator.start();

        assertThat(enumerator.getSnapshot(), equalTo(versionAsOfSnapshot));
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).snapshot();
        verify(deltaLog).getSnapshotForVersionAsOf(versionAsOf);
    }

    @Test
    public void shouldUseTimestampAsOfSnapshot() {
        long timestampAsOf = System.currentTimeMillis();

        sourceOptions.addOption(TIMESTAMP_AS_OF.key(), timestampAsOf);
        when(deltaLog.getSnapshotForTimestampAsOf(timestampAsOf)).thenReturn(timestampAsOfSnapshot);

        enumerator = new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceOptions);

        List<DeltaSourceSplit> mockSplits = mockSplits();
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenReturn(mockSplits);

        enumerator.start();

        assertThat(enumerator.getSnapshot(), equalTo(timestampAsOfSnapshot));
        verify(deltaLog).getSnapshotForTimestampAsOf(timestampAsOf);
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
    }


    @Test
    public void shouldHandleFailedReader() {
        setupEnumeratorWithHeadSnapshot();

        // Mock reader failure.
        when(enumContext.registeredReaders()).thenReturn(Collections.emptyMap());

        int subtaskId = 1;
        enumerator.handleSplitRequest(subtaskId, "testHost");
        verify(enumContext, never()).assignSplit(any(DeltaSourceSplit.class), anyInt());
    }

    @Test
    public void shouldSignalNoMoreSplitsIfNone() {
        int subtaskId = 1;
        setupEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        enumerator.handleSplitRequest(subtaskId, "testHost");
        verify(enumContext).signalNoMoreSplits(subtaskId);
    }

    @Test
    public void shouldAssignSplitToReader() {
        int subtaskId = 1;
        setupEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        String host = "testHost";
        when(splitAssigner.getNext(host)).thenReturn(Optional.of(split))
            .thenReturn(Optional.empty());

        // handle request split when there is a split to assign
        enumerator.handleSplitRequest(subtaskId, host);
        verify(enumContext).assignSplit(split, subtaskId);
        verify(enumContext, never()).signalNoMoreSplits(anyInt());

        // check that we clear split from enumerator after assigning them.
        enumerator.handleSplitRequest(subtaskId, host);
        verify(enumContext).assignSplit(split, subtaskId); // the one from previous assignment.
        verify(enumContext).signalNoMoreSplits(subtaskId);
    }

    @Test
    public void shouldAddSplitBack() {
        int subtaskId = 1;
        setupEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        String testHost = "testHost";
        enumerator.handleSplitRequest(subtaskId, testHost);
        verify(enumContext).signalNoMoreSplits(subtaskId);

        enumerator.addSplitsBack(Collections.singletonList(split), subtaskId);

        //capture the assigned split to mock assigner and use it in getNext mock
        verify(splitAssigner).addSplits(splitsCaptor.capture());

        when(splitAssigner.getNext(testHost)).thenReturn(
            Optional.ofNullable(splitsCaptor.getValue().get(0)));
        enumerator.handleSplitRequest(subtaskId, testHost);
        verify(enumContext).assignSplit(split, subtaskId);
    }

    @Test
    public void shouldReadInitialSnapshot() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        enumerator = new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceOptions);

        List<DeltaSourceSplit> mockSplits = mockSplits();
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenReturn(mockSplits);

        enumerator.start();

        verify(splitAssigner).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue(), equalTo(mockSplits));
    }

    @Test
    public void shouldNotProcessAlreadyProcessedPaths() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        enumerator = new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceOptions);

        AddFile mockAddFile = mock(AddFile.class);
        when(mockAddFile.getPath()).thenReturn("add/file/path.parquet");
        when(headSnapshot.getAllFiles()).thenReturn(Collections.singletonList(mockAddFile));

        mockFileEnumerator();

        enumerator.start();

        verify(splitAssigner).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue().size(), equalTo(1));

        // Reprocess the same data again
        enumerator.start();

        verify(splitAssigner, times(2)).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue().isEmpty(), equalTo(true));

    }

    // Moc FileEnumerator to check and execute SplitFilter instance used by
    // BoundedDeltaSourceSplitEnumerator.
    private void mockFileEnumerator() {
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenAnswer((Answer<List<DeltaSourceSplit>>) invocation -> {
                AddFileEnumeratorContext context = invocation.getArgument(0);
                SplitFilter<Path> filter = invocation.getArgument(1);

                // id is not a primitive int just to trick Java
                // since we need to use final objects in streams.
                AtomicInteger id = new AtomicInteger(0);
                return context.getAddFiles().stream()
                    .filter(addFile -> filter.test(new Path(addFile.getPath())))
                    .map(addFile ->
                        new DeltaSourceSplit(addFile.getPartitionValues(),
                            String.valueOf(id.incrementAndGet()), new Path(addFile.getPath()),
                            0L, 0L))
                    .collect(Collectors.toList());
            });
    }

    private List<DeltaSourceSplit> mockSplits() {
        return Arrays.asList(mock(DeltaSourceSplit.class), mock(DeltaSourceSplit.class),
            mock(DeltaSourceSplit.class),
            mock(DeltaSourceSplit.class));
    }

    private void setupEnumeratorWithHeadSnapshot() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        enumerator = new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumerator, splitAssigner, DeltaSinkTestUtils.getHadoopConf(),
            enumContext, sourceOptions);
    }

}

