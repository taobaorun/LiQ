/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License
 */

package com.jiaxy.liq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/18 14:15
 */
public class MappedFile {

    private static final Logger logger = LoggerFactory.getLogger(MappedFile.class);

    //file start offset
    private final String fileName;

    private long fileStartOffset;

    //one file size
    private final int fileSize;

    private File file;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    //file wrote position
    private final AtomicInteger wrotePosition = new AtomicInteger(0);

    //file flushed position
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    public MappedFile(String fileName, int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        build();
    }


    public boolean appendData(byte[] data) {
        int position = wrotePosition.get();
        try {
            fileChannel.write(ByteBuffer.wrap(data));
            int newPos = position + data.length;
            fileChannel.position(newPos);
            wrotePosition.set(newPos);
        } catch (IOException e) {
            logger.error("append data for {}.", fileName, e);
            return false;
        }
        return true;
    }


    /**
     * @param dataHandler
     * @return
     */
    public AppendMeta appendInByteBuffer(AppendFunction dataHandler) {
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        //set wrote position
        byteBuffer.position(wrotePosition.get());
        AppendMeta appendMeta = dataHandler.apply(byteBuffer, getFileStartOffset() + wrotePosition.get(), fileSize - wrotePosition.get());
        wrotePosition.addAndGet(appendMeta.getWroteBytes());
        return appendMeta;
    }

    public ByteBuffer sliceMappedByteBuffer() {
        return mappedByteBuffer.slice();
    }

    /**
     * @param pos  relative position in a file
     * @param size stored message size
     * @return
     */
    public SelectedMappedFileSection selectMappedFileSection(int pos, int size) {
        int wrotePos = getWrotePosition();
        if (pos >= 0 && (pos + size) <= wrotePos) {
            ByteBuffer byteBuffer = mappedByteBuffer.slice();
            byteBuffer.position(pos);
            ByteBuffer selected = byteBuffer.slice();
            selected.limit(size);
            return new SelectedMappedFileSection(selected, size);
        }
        return null;
    }


    public long getFileStartOffset() {
        return fileStartOffset;
    }


    /**
     * @return true if the file wrote to the file size
     */
    public boolean isFull() {
        if (wrotePosition.get() == fileSize) {
            return true;
        }
        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }


    /**
     * destroy the file
     *
     * @return true if destroyed success, else false
     */
    public boolean destroy() {
        if (file != null) {
            try {
                file.delete();
                fileChannel.close();
                mappedByteBuffer = null;
            } catch (IOException e) {
                logger.error("destroy file for {} error.", fileName, e);
                return false;
            }
        }
        return true;
    }

    /**
     * @return
     */
    public int flush() {
        int wrotePos = getWrotePosition();
        mappedByteBuffer.force();
        flushedPosition.set(wrotePos);
        return flushedPosition.get();
    }


    /**
     * the file just loaded.
     */
    public void loaded() {
        this.wrotePosition.set(fileSize);
        this.flushedPosition.set(fileSize);
    }


    /**
     * the file is ready
     * <p>
     * update {@code wrotePosition}
     * <p>
     * update {@code flushedPosition}
     *
     * @param pos relative position in the file
     */
    public void ready(int pos) {
        this.wrotePosition.set(pos);
        this.flushedPosition.set(pos);
    }

    public int getFileSize() {
        return fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    private void build() throws IOException {
        try {
            this.file = new File(fileName);
            this.fileStartOffset = Long.parseLong(file.getName());
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
            mappedByteBuffer = fileChannel.map(READ_WRITE, 0, fileSize);
        } catch (FileNotFoundException e) {
            logger.error("create file for {} error.", fileName, e);
            throw e;
        } catch (IOException e) {
            logger.error("create mapped file for {} error.", fileName, e);
            throw e;
        }
    }


}
