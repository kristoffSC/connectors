package io.delta.flink.source;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.actions.AddFile;

class DeltaFileEnumerator implements AddFileEnumerator<DeltaSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaFileEnumerator.class);

    /**
     * The current Id as a mutable string representation. This covers more values than the integer
     * value range, so we should never overflow.
     */
    private final char[] currentId = "0000000000".toCharArray();

    @Override
    public List<DeltaSourceSplit> enumerateSplits(AddFileEnumeratorContext context)
        throws IOException {
        final ArrayList<DeltaSourceSplit> splits = new ArrayList<>();

        for (AddFile addFile : context.getAddFiles()) {

            String addFilePath = addFile.getPath();
            URI addFileUri = URI.create(addFilePath);
            if (!addFileUri.isAbsolute()) {
                addFileUri = URI.create(context.getTablePath() + addFilePath);
            }

            Path path = new Path(addFileUri);
            final FileSystem fs = path.getFileSystem();
            final FileStatus status = fs.getFileStatus(path);
            convertToSourceSplits(status, fs, addFile.getPartitionValues(), splits);
        }

        return splits;
    }

    // ------------------------------------------------------------------------
    //  Copied from Flink's BlockSplittingRecursiveEnumerator and adjusted.
    // ------------------------------------------------------------------------
    private void convertToSourceSplits(final FileStatus fileStatus, final FileSystem fileSystem,
        Map<String, String> partitionValues, final List<DeltaSourceSplit> target)
        throws IOException {

        final BlockLocation[] blocks = getBlockLocationsForFile(fileStatus, fileSystem);
        if (blocks == null) {
            target.add(
                new DeltaSourceSplit(
                    partitionValues,
                    getNextId(),
                    fileStatus.getPath(),
                    0L,
                    fileStatus.getLen()));
        } else {
            for (BlockLocation block : blocks) {
                target.add(new DeltaSourceSplit(
                    partitionValues,
                    getNextId(),
                    fileStatus.getPath(),
                    block.getOffset(),
                    block.getLength(),
                    block.getHosts()));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Copied as is from Flink's BlockSplittingRecursiveEnumerator
    // ------------------------------------------------------------------------

    private String getNextId() {
        // because we just increment numbers, we increment the char representation directly,
        // rather than incrementing an integer and converting it to a string representation
        // every time again (requires quite some expensive conversion logic).
        incrementCharArrayByOne(currentId, currentId.length - 1);
        return new String(currentId);
    }

    private void incrementCharArrayByOne(char[] array, int pos) {
        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1);
        }
        array[pos] = c;
    }

    private BlockLocation[] getBlockLocationsForFile(FileStatus file, FileSystem fs)
        throws IOException {
        final long len = file.getLen();

        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        if (blocks == null || blocks.length == 0) {
            return null;
        }

        // A cheap check whether we have all blocks.
        // We don't check whether the blocks fully cover the file (too expensive)
        // but make some sanity checks to catch early the common cases where incorrect
        // block info is returned by the implementation.

        long totalLen = 0L;
        for (BlockLocation block : blocks) {
            totalLen += block.getLength();
        }
        if (totalLen != len) {
            LOG.warn(
                "Block lengths do not match file length for {}. File length is {}, blocks are {}",
                file.getPath(), len, Arrays.toString(blocks));
            return null;
        }

        return blocks;
    }

    // ------------------------------------------------------------------------
    //  End of code copied from Flink's BlockSplittingRecursiveEnumerator
    // ------------------------------------------------------------------------
}
