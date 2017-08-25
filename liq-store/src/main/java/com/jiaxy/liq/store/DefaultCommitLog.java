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
import com.jiaxy.liq.core.message.MessageMeta;
import com.jiaxy.liq.core.message.MessageProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import static com.jiaxy.liq.common.SystemTime.nowMillis;
import static com.jiaxy.liq.core.message.MessageProtocol.PADDING_MESSAGE_LENGTH;
import static com.jiaxy.liq.store.AppendMeta.AppendStatus.APPEND_OK;
import static com.jiaxy.liq.store.AppendMeta.AppendStatus.END_OF_FILE;
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

    private final MessageProtocol messageProtocol;

    private final MessageStoreConfig messageStoreConfig;

    public DefaultCommitLog(MessageStoreConfig storeConfig) {
        this.messageStoreConfig = storeConfig;
        this.mappedFileQueue = new MappedFileQueue(storeConfig.getStorePath(), storeConfig.getCommitLogFileSize());
        this.putMessageLock = new DefaultPutMessageLock();
        this.messageProtocol = new MessageProtocol();
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
            AppendMeta appendMeta = mappedFile.appendInByteBuffer((byteBuffer, writeOffset, leftSize) -> appendMessage(byteBuffer, writeOffset, leftSize, message));
            switch (appendMeta.getStatus()) {
                case APPEND_OK:
                    break;
                case END_OF_FILE:
                    mappedFile = mappedFileQueue.getLastMappedFile(0, true);
                    if (mappedFile == null) {
                        logger.error("create mapped file error.when the last file is full.");
                        return new PutMessageResult(CREATE_MAPPED_FILE_ERROR);
                    }
                    appendMeta = mappedFile.appendInByteBuffer((byteBuffer, writeOffset, leftSize) -> appendMessage(byteBuffer, writeOffset, leftSize, message));
                    break;
            }
            putMessageResult.setAppendResult(appendMeta);
            return putMessageResult;
        } catch (Exception e) {
            logger.error("put [{}] message error.", message.getMeta().getTopic(), message.getMeta().getTopic(), e);
            return new PutMessageResult(PUT_FAILED);
        } finally {
            putMessageLock.unLock();
        }
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
        recoverFromNormalStatus();
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

    /**
     * append one message
     *
     * @param byteBuffer  mapped file context:sliced buffer
     * @param writeOffset mapped file context:append message physical offset
     * @param leftSize    mapped file context:the file left space
     * @param message     the message will be appended
     * @return
     */
    private AppendMeta appendMessage(ByteBuffer byteBuffer, long writeOffset, int leftSize, Message message) {
        String msgId = messageProtocol.createMessageId(message, writeOffset);
        message.getMeta().setMsgId(msgId);
        byte[] topicData = message.getMeta().getTopic().getBytes();
        int totalLength = messageProtocol.calcTotalLength(message.getData().length, topicData.length);
        message.getMeta().setTotalLength(totalLength);
        message.getMeta().setTopicData(topicData);
        message.getMeta().setCommitLogOffset(writeOffset);
        long start = nowMillis();
        //the mapped file have not enough space
        if (totalLength + PADDING_MESSAGE_LENGTH >= leftSize) {
            messageProtocol.writePaddingMessage(byteBuffer, leftSize);
            AppendMeta appendResult = new AppendMeta(END_OF_FILE, writeOffset,
                    leftSize,
                    msgId,
                    0,
                    message.getMeta().getStoredTimestamp(),
                    (int) (nowMillis() - start));
            return appendResult;
        }
        messageProtocol.writeMessage(message, byteBuffer);
        return new AppendMeta(APPEND_OK, writeOffset,
                totalLength,
                msgId,
                0,
                message.getMeta().getStoredTimestamp(),
                (int) (nowMillis() - start));

    }


    /**
     * recover for commit log exit normally
     */
    private void recoverFromNormalStatus() {
        List<MappedFile> mappedFiles = mappedFileQueue.getMappedFiles();
        if (mappedFiles != null && !mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - messageStoreConfig.getRecoverBaseOnLastFileNum();
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
                while (true) {
                    MessageMeta messageMeta = messageProtocol.readMessageMeta(byteBuffer, true);
                    if (messageMeta.isPadding()) {
                        break;
                    } else if (messageMeta.getTotalLength() == 0) {
                        break;
                    }
                    pos += messageMeta.getTotalLength();
                }
            }
            //the last mapped file ready
            mappedFile.ready(pos);
            mappedFileQueue.setFlushedPosition(fileStartOffset + pos);
        }
    }

    /**
     * recover for commit log exit abnormally
     */
    private void recoverFromAbnormalStatus() {

    }

}
