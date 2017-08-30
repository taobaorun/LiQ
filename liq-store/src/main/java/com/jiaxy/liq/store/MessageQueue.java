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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/28 17:57
 */
public class MessageQueue {

    private static final Logger logger = LoggerFactory.getLogger(MessageQueue.class);

    public static final int ITEM_SIZE = 20;

    private static final ByteBuffer itemByteBuffer = ByteBuffer.allocate(ITEM_SIZE);

    private final String topic;

    private final int queueId;

    private final String storePath;

    private final int mappedFileSize;

    private final MappedFileQueue mappedFileQueue;

    //commit log max physical offset
    private long maxCommitLogPhyOffset = -1;

    //the start offset of one message queue
    private volatile long minMessageQueueOffset = 0;

    private final MessageStoreConfig storeConfig;


    public MessageQueue(String topic, int queueId, MessageStoreConfig storeConfig) {
        this.topic = topic;
        this.storeConfig = storeConfig;
        this.queueId = queueId;
        this.storePath = this.storeConfig.getMessageQueueStorePath();
        this.mappedFileSize = this.storeConfig.getMessageQueueFileSize();
        String queuePath = this.storePath + File.separator + topic + File.separator + queueId;
        mappedFileQueue = new MappedFileQueue(queuePath, mappedFileSize);
    }


    public boolean load() {
        return mappedFileQueue.load();
    }


    /**
     * @param phyOffset
     * @param size
     * @param tagsCode
     * @param queueIndex relative queue offset
     * @return
     */
    public boolean putMessageQueue(long phyOffset, int size, long tagsCode, long queueIndex) {
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
        long expectQueuePhyOffset = ITEM_SIZE * queueIndex;
        if (mappedFile == null) {
            mappedFile = mappedFileQueue.getLastMappedFile(expectQueuePhyOffset, true);
            if (mappedFile != null) {
                if (queueIndex != 0) {
                    minMessageQueueOffset = expectQueuePhyOffset;
                    mappedFileQueue.setFlushedPosition(expectQueuePhyOffset);
                    fillPrePadding(mappedFile, expectQueuePhyOffset);
                }
            }
        }
        if (mappedFile == null) {
            logger.error("message queue get mapped file failed.");
            return false;
        }
        if (mappedFile.isFull()) {
            mappedFile = mappedFileQueue.getLastMappedFile(expectQueuePhyOffset, true);
        }
        long currentQueuePhyOffset = mappedFile.getFileStartOffset() + mappedFile.getWrotePosition();
        if (expectQueuePhyOffset != currentQueuePhyOffset) {
            logger.warn("message queue order maybe wrong.expect offset:{},current queue offset:{} Topic:{},QID:{}",
                    expectQueuePhyOffset,
                    currentQueuePhyOffset,
                    topic,
                    queueId);
        }
        itemByteBuffer.flip();
        itemByteBuffer.limit(ITEM_SIZE);
        itemByteBuffer.putLong(phyOffset);
        itemByteBuffer.putInt(size);
        itemByteBuffer.putLong(tagsCode);
        mappedFile.appendData(itemByteBuffer.array());
        maxCommitLogPhyOffset = phyOffset;
        return true;
    }


    /**
     *
     */
    public void recover() {
        List<MappedFile> mappedFiles = mappedFileQueue.getMappedFiles();
        if (mappedFiles != null && !mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - storeConfig.getRecoverBaseOnLastFileNum();
            if (index < 0) {
                index = 0;
            }
            MappedFile mappedFile = null;
            int pos = 0;
            long fileStartOffset = 0;
            for (int i = index; i < mappedFiles.size(); i++) {
                mappedFile = mappedFiles.get(i);
                pos = 0;
                fileStartOffset = mappedFile.getFileStartOffset();
                ByteBuffer byteBuffer = mappedFile.sliceMappedByteBuffer();
                for (int j = 0; j < mappedFileSize; j += ITEM_SIZE) {
                    long phyOffset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    if (phyOffset >= 0 && size > 0) {
                        pos += ITEM_SIZE;
                        this.maxCommitLogPhyOffset = phyOffset;
                    } else {
                        logger.info("recover {} message queue finished.consume queue pos:{}",
                                mappedFile.getFileName(),
                                pos);
                        break;
                    }

                }
            }
            //the last mapped file ready
            mappedFile.ready(pos);
            mappedFileQueue.setFlushedPosition(fileStartOffset + pos);
        }
    }


    /**
     * @return message queue max index
     */
    public long getMessageQueueMaxIndex() {
        return this.mappedFileQueue.getMaxPhyOffset() / ITEM_SIZE;
    }

    public long getMinMessageQueueOffset() {
        return minMessageQueueOffset;
    }

    public long getMaxCommitLogPhyOffset() {
        return maxCommitLogPhyOffset;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    /**
     * @param mappedFile
     * @param queueOffset
     */
    private void fillPrePadding(MappedFile mappedFile, long queueOffset) {
        int padding = (int) (queueOffset % mappedFileSize);
        for (int i = 0; i < padding; i += ITEM_SIZE) {
            itemByteBuffer.flip();
            itemByteBuffer.putLong(0L);
            itemByteBuffer.putInt(Integer.MAX_VALUE);
            itemByteBuffer.putLong(0L);
            mappedFile.appendData(itemByteBuffer.array());
        }
    }


}
