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
 *    limitations under the License.
 */

package com.jiaxy.liq.store;

import com.jiaxy.liq.common.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Description: <br/>
 * <p/>
 * mapped file container
 * <br/>
 *
 * @Date: 2017/08/21 15:23
 */
public class MappedFileQueue {

    private static final Logger logger = LoggerFactory.getLogger(MappedFileQueue.class);

    private final String storePath;

    private final int mappedFileSize;

    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    //flushed position.
    private long flushedPosition;


    public MappedFileQueue(String storePath, int mappedFileSize) {
        this.mappedFileSize = mappedFileSize;
        this.storePath = storePath;
    }

    public MappedFile getLastMappedFile() {
        if (mappedFiles != null && !mappedFiles.isEmpty()) {
            return mappedFiles.get(mappedFiles.size() - 1);
        }
        return null;
    }

    /**
     * get the mapped file by the physical offset
     *
     * @param phyOffset     the physical offset
     * @param newMappedFile create new mapped file if true
     * @return
     */
    public MappedFile getLastMappedFile(long phyOffset, boolean newMappedFile) {
        long startOffset = -1;
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile == null) {
            startOffset = phyOffset - phyOffset % mappedFileSize;
        } else if (mappedFile != null && mappedFile.isFull()) {
            startOffset = mappedFile.getFileStartOffset() + mappedFileSize;
        }
        if (startOffset != -1 && newMappedFile) {
            String newFileName = storePath + File.separator + FileUtil.getFileName(startOffset);
            try {
                mappedFile = new MappedFile(newFileName, mappedFileSize);
            } catch (IOException e) {
                logger.error("create mapped file error.", e);
            }
            mappedFiles.add(mappedFile);
        }
        return mappedFile;
    }


    /**
     * find mapped file by the physical offset
     *
     * @param phyOffset
     * @return
     */
    public MappedFile findMappedFile(long phyOffset) {
        MappedFile firstFile = findFirstMappedFile();
        if (firstFile == null) {
            return null;
        }
        int index = (int) (phyOffset / mappedFileSize - firstFile.getFileStartOffset() / mappedFileSize);
        if (index < 0 || index >= mappedFiles.size()) {
            logger.error("can't find mapped file for the {}.the mapped file queue size is {}.but the inferred index is {}.",
                    phyOffset,
                    mappedFiles.size(),
                    index);
            return null;
        }
        return mappedFiles.get(index);
    }

    public MappedFile findFirstMappedFile() {
        if (!mappedFiles.isEmpty()) {
            return mappedFiles.get(0);
        }
        return null;
    }


    public boolean flush() {
        MappedFile mappedFile = findMappedFile(flushedPosition);
        if (mappedFile != null) {
            int offset = mappedFile.flush();
            long flushedOffset = mappedFile.getFileStartOffset() + offset;
            boolean result = flushedPosition == flushedOffset;
            flushedPosition = flushedOffset;
            return result;
        }
        return true;
    }

    /**
     * load stored files
     *
     * @return
     */
    public boolean load() {
        File storeDir = new File(storePath);
        File[] files = storeDir.listFiles();
        if (files != null && files.length != 0) {
            Arrays.sort(files);
        }
        for (File file : files) {
            if (file.length() != mappedFileSize) {
                logger.warn("the file {}'s length is not equal the configured mapped file size[{}],ignore behind files.",
                        file.getName(), mappedFileSize);
                return true;
            }
            try {
                MappedFile mappedFile = new MappedFile(file.getAbsolutePath(), mappedFileSize);
                mappedFile.loaded();
                mappedFiles.add(mappedFile);
            } catch (IOException e) {
                logger.error("load the file [{}] error.", file.getName(), e);
                return false;
            }
        }
        return true;
    }

    protected void setFlushedPosition(long flushedPosition) {
        this.flushedPosition = flushedPosition;
    }

    protected List<MappedFile> getMappedFiles() {
        return new ArrayList<>(mappedFiles);
    }
}
