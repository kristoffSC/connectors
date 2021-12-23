package io.delta.flink.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeltaFileEnumerator implements FileEnumerator {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaFileEnumerator.class);

    /**
     * The current Id as a mutable string representation. This covers more values than the integer
     * value range, so we should never overflow.
     */
    private final char[] currentId = "0000000000".toCharArray();


    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
        throws IOException {
        final ArrayList<FileSourceSplit> splits = new ArrayList<>();

        for (Path path : paths) {
            final FileSystem fs = path.getFileSystem();
            final FileStatus status = fs.getFileStatus(path);
            convertToSourceSplits(status, fs, splits);
        }

        return splits;
    }

    // ------------------------------------------------------------------------
    //  Copied from Flink's BlockSplittingRecursiveEnumerator and adjusted.
    // ------------------------------------------------------------------------
    private void convertToSourceSplits(final FileStatus fileStatus, final FileSystem fileSystem,
        final List<FileSourceSplit> target) throws IOException {

        final BlockLocation[] blocks = getBlockLocationsForFile(fileStatus, fileSystem);
        if (blocks == null) {
            target.add(
                new FileSourceSplit(getNextId(), fileStatus.getPath(), 0L, fileStatus.getLen()));
        } else {
            for (BlockLocation block : blocks) {
                target.add(new FileSourceSplit(
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
