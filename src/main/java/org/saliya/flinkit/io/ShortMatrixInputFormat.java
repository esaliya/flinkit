package org.saliya.flinkit.io;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.*;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.hadoop.shaded.com.google.common.io
    .LittleEndianDataInputStream;
import org.apache.flink.types.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Input format class to represent an NxN binary matrix
 * stored as a contiguous Short values, with no meta info
 */
public class ShortMatrixInputFormat extends FileInputFormat<Short[]> {
    private static final long serialVersionUID = 1L;

    /**
     * The log.
     */
    private static final Logger LOG = LoggerFactory
        .getLogger(ShortMatrixInputFormat.class);

    private boolean isBigEndian = true;
    private boolean divideByShortMax;
    private int globalColumnCount;

    private boolean isRead = false;

    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits)
        throws IOException {
        final FileSystem fs = this.filePath.getFileSystem();
        final FileStatus file = fs.getFileStatus(this.filePath);

        FileInputSplit[] splits = new FileInputSplit[minNumSplits];
        int q = globalColumnCount / minNumSplits;
        int r = globalColumnCount % minNumSplits;

        long start = 0,length;
        BlockLocation[] blocks;
        for (int i = 0; i < minNumSplits; ++i){
            blocks = fs.getFileBlockLocations(file, 0, file.getLen());
            if (blocks.length != 1){
                throw new RuntimeException("File blocks should be 1 for local file system");
            }
            length = (q+(i < r ? 1 : 0))*globalColumnCount*Short.BYTES;
            FileInputSplit fis = new FileInputSplit(i, this.filePath, start, length, blocks[0].getHosts());
            splits[i] = fis;
            start+=length;
        }
        return splits;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return isRead;
    }

    @Override
    public Short[] nextRecord(Short[] reuse) throws IOException {
        int shortLength = (int)(this.splitLength / Short.BYTES);
        reuse = new Short[shortLength];

        if (isBigEndian) {
            DataInputStream dis = new DataInputStream(this.stream);
            for (int i = 0; i < shortLength; ++i) {
                reuse[i] = dis.readShort();
            }
        } else {
            LittleEndianDataInputStream ldis = new LittleEndianDataInputStream(this.stream);
            for (int i = 0; i < shortLength; ++i) {
                reuse[i] = ldis.readShort();
            }
        }
        isRead = true;
        return reuse;
    }

    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
        // This uses an input stream, later see how to change to
        // memory maps, will have to change nextRecord() method as well
        super.open(fileSplit);
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        // Read any other own configuration parameters
        // like transform class, etc. - later
    }

    @Override
    public void setFilePath(String filePath) {
        super.setFilePath(filePath);
        throwExceptionIfDistributedFS();
    }

    private void throwExceptionIfDistributedFS() {
        try {
            if (this.filePath.getFileSystem().isDistributedFS()){
                throw new IllegalArgumentException("Distributed file systems are not supported in " + this.getClass().getSimpleName());
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setFilePath(Path filePath) {
        super.setFilePath(filePath);
        throwExceptionIfDistributedFS();
    }

    public boolean isBigEndian() {
        return isBigEndian;
    }

    public void setBigEndian(boolean bigEndian) {
        isBigEndian = bigEndian;
    }

    public boolean isDivideByShortMax() {
        return divideByShortMax;
    }

    public void setDivideByShortMax(boolean divideByShortMax) {
        this.divideByShortMax = divideByShortMax;
    }

    public int getGlobalColumnCount() {
        return globalColumnCount;
    }

    public void setGlobalColumnCount(int globalColumnCount) {
        this.globalColumnCount = globalColumnCount;
    }
}
