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

import com.jiaxy.liq.core.message.Message;
import com.jiaxy.liq.core.message.MessageProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.jiaxy.liq.common.SystemTime.nowMillis;
import static com.jiaxy.liq.core.message.MessageProtocol.BLANK_MESSAGE_LENGTH;
import static com.jiaxy.liq.store.PutMessageStatus.*;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/21 15:12
 */
public class DefaultCommitLog implements CommitLog {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommitLog.class);


    private final MappedFileQueue mappedFileQueue;


    private final PutMessageLock putMessageLock;

    private final MessageProtocol msgFileProtocol;

    private final MessageStoreConfig messageStoreConfig;

    public DefaultCommitLog(MessageStoreConfig storeConfig) {
        this.messageStoreConfig = storeConfig;
        this.mappedFileQueue = new MappedFileQueue(storeConfig.getStorePath(), storeConfig.getCommitLogFileSize());
        this.putMessageLock = new DefaultPutMessageLock();
        this.msgFileProtocol = new MessageProtocol();
    }

    public PutMessageResult putMessage(Message message) {
        try {
            putMessageLock.lock();
            message.getMeta().setStoredTimestamp(nowMillis());
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
            if (mappedFile == null || mappedFile.isFull()) {
                mappedFile = mappedFileQueue.getLastMappedFile(0, true);
            }
            if (mappedFile == null) {
                logger.error("create mapped file error");
                return new PutMessageResult(CREATE_MAPPED_FILE_ERROR);
            }
            PutMessageResult putMessageResult = new PutMessageResult(PUT_OK);
            AppendMeta appendMeta = mappedFile.appendInByteBuffer((byteBuffer, writeOffset, leftSize) -> {
                String msgId = msgFileProtocol.createMessageId(message, writeOffset);
                message.getMeta().setMsgId(msgId);
                byte[] topicData = message.getMeta().getTopic().getBytes();
                int totalLength = msgFileProtocol.calcTotalLength(message.getData().length, topicData.length);
                message.getMeta().setTotalLength(totalLength);
                message.getMeta().setTopicData(topicData);
                message.getMeta().setCommitLogOffset(writeOffset);
                long start = nowMillis();
                //the mapped file have not enough space
                if (totalLength + BLANK_MESSAGE_LENGTH >= leftSize) {
                    msgFileProtocol.writeBlankMessage(byteBuffer, leftSize);
                    AppendMeta appendResult = new AppendMeta(writeOffset,
                            leftSize,
                            msgId,
                            0,
                            message.getMeta().getStoredTimestamp(),
                            (int) (nowMillis() - start));
                    putMessageResult.setStatus(END_OF_FILE);
                    return appendResult;
                }
                msgFileProtocol.writeMessage(message, byteBuffer);
                return new AppendMeta(writeOffset,
                        totalLength,
                        msgId,
                        0,
                        message.getMeta().getStoredTimestamp(),
                        (int) (nowMillis() - start));
            });
            putMessageResult.setAppendResult(appendMeta);
            return putMessageResult;
        } catch (Exception e) {
            logger.error("put [%s] message error.", message.getMeta().getTopic(), message.getMeta().getTopic());
        } finally {
            putMessageLock.unLock();
        }
        return null;
    }

    @Override
    public SelectedMappedFileSection getMessage(long phyOffset, int size) {
        MappedFile mappedFile = mappedFileQueue.findMappedFile(phyOffset);
        if (mappedFile != null) {
            int pos = (int) (phyOffset % messageStoreConfig.getCommitLogFileSize());
            return mappedFile.selectMappedFileSection(pos, size);
        }
        return null;
    }

    @Override
    public void flush() {
        mappedFileQueue.flush();
    }

    public boolean load() {
        if (!checkStoreDir()) {
            logger.error("the store dir is illegal");
            return false;
        }
        boolean rs = mappedFileQueue.load();
        if (rs) {
            logger.info("CommitLog loaded success.");
        } else {
            logger.error("CommitLog loaded success.");
        }
        return rs;
    }

    public void recover() {

    }



    private boolean checkStoreDir() {
        File storeDir = new File(messageStoreConfig.getStorePath());
        if (storeDir.exists()) {
            return true;
        } else {
            try {
                storeDir.mkdir();
                return true;
            } catch (Exception e) {
                logger.error("make the store dir:{} error.", messageStoreConfig.getStorePath(), e);
                return false;
            }
        }
    }
}
