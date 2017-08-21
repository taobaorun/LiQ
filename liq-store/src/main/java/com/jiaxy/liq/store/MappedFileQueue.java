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

import java.io.File;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/21 15:23
 */
public class MappedFileQueue {

    private final String storePath;

    private final int mappedFileSize;

    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();


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
    public MappedFile getMappedFile(long phyOffset, boolean newMappedFile) {
        long startOffset = -1;
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile == null) {
            startOffset = phyOffset - phyOffset % mappedFileSize;
        } else if (mappedFile != null && mappedFile.isFull()) {
            startOffset = mappedFile.getFileStartOffset() + mappedFileSize;
        }
        if (startOffset != -1 && newMappedFile) {
            String newFileName = storePath + File.separator + FileUtil.getFileName(startOffset);
            mappedFile = new MappedFile(newFileName, mappedFileSize);
            mappedFiles.add(mappedFile);
        }
        return mappedFile;
    }
}
