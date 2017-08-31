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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.jiaxy.liq.store.GetMessageStatus.*;
import static com.jiaxy.liq.store.MessageQueue.ITEM_SIZE;
import static com.jiaxy.liq.store.PutMessageStatus.PUT_OK;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/29 11:39
 */
public class DefaultMessageStore implements MessageStore {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageStore.class);

    private final MessageEventPipeline pipeline;

    private final CommitLog commitLog;

    private final MessageStoreConfig storeConfig;

    private final MessageQueueHolder messageQueueHolder;


    public DefaultMessageStore(MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.commitLog = new DefaultCommitLog(this.storeConfig);
        this.messageQueueHolder = new MessageQueueHolder(this.storeConfig, (DefaultCommitLog) this.commitLog);
        this.pipeline = new MessageEventPipeline(storeConfig, messageQueueHolder, 1000);
    }

    @Override
    public boolean load() {
        boolean rs = commitLog.load();
        rs = rs && messageQueueHolder.load();
        if (rs) {
            recover();
        }
        return rs;
    }

    @Override
    public void start() {
        pipeline.start();
    }

    @Override
    public void shutdown() {

    }

    @Override
    public PutMessageResult putMessage(Message message) {
        PutMessageResult result = commitLog.putMessage(message);
        if (result.getStatus() == PUT_OK) {
            MessageEvent event = new MessageEvent(message.getMeta().getTopic(),
                    message.getMeta().getQueueId(),
                    message.getMeta().getTags().hashCode(),
                    result);
            pipeline.messagePut(event);
        }
        return result;
    }


    @Override
    public GetMessageResult getMessage(String topic, Integer queueId, long queueIndex, int maxMessageSize) {
        MessageQueue messageQueue = messageQueueHolder.findMessageQueue(topic, queueId);
        GetMessageResult result = new GetMessageResult();
        GetMessageStatus status = null;
        long nextQueueIndex = 0;
        long queueMinIndex = 0;
        long queueMaxIndex = 0;
        if (messageQueue != null) {
            queueMaxIndex = messageQueue.getMessageQueueMaxIndex();
            queueMinIndex = messageQueue.getMessageQueueMinIndex();
            if (queueMaxIndex == 0) {
                status = NO_MESSAGE_IN_MQ;
                nextQueueIndex = nextQueueIndex(queueIndex, 0);
            } else if (queueIndex < queueMinIndex) {
                status = MQ_INDEX_TOO_SMALL;
                nextQueueIndex = nextQueueIndex(queueIndex, queueMinIndex);
            } else if (queueIndex >= queueMaxIndex) {
                status = MQ_INDEX_OVERFLOW;
                nextQueueIndex = nextQueueIndex(queueIndex, queueMaxIndex);
            } else {
                SelectedMappedFileSection selectedSection = messageQueue.readMessageQueue(queueIndex);
                if (selectedSection != null) {
                    int maxMessageCount = Math.max(800 * ITEM_SIZE, maxMessageSize * ITEM_SIZE);
                    int i = 0;
                    long nextMQStartIndex = -1;
                    ByteBuffer byteBuffer = selectedSection.getByteBuffer();
                    for (; i < selectedSection.getSize() && i < maxMessageCount; i += ITEM_SIZE) {
                        //message count or message byte size
                        if (isEnough(result, maxMessageSize)) {
                            break;
                        }
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        long tagsCode = byteBuffer.getLong();
                        //ignore the physical offset less than the message queue next file start queue index
                        if (nextMQStartIndex != -1 && phyOffset < nextMQStartIndex) {
                            continue;
                        }
                        SelectedMappedFileSection message = commitLog.getMessage(phyOffset, size);
                        if (message == null) {
                            nextMQStartIndex = messageQueue.rollNextFile(queueIndex);
                            continue;
                        }
                        result.addMessage(message);
                        status = FOUND;
                        //rest
                        nextMQStartIndex = -1;
                    }
                    nextQueueIndex = queueIndex + (i / ITEM_SIZE);
                } else {
                    status = MQ_SHOULD_NOT_NULL;
                    nextQueueIndex = nextQueueIndex(queueIndex, messageQueue.rollNextFile(queueIndex));
                    logger.warn("get message topic:{},request message queue index:{},min queue index :{},max queue index:{}",
                            topic, queueIndex, queueMinIndex, queueMaxIndex);
                }
            }

        } else {
            status = MQ_NOT_FOUND;
            nextQueueIndex = nextQueueIndex(queueIndex, 0);
        }
        result.setStatus(status);
        result.setQueueMinIndex(queueMinIndex);
        result.setQueueMaxIndex(queueMaxIndex);
        result.setNextQueueIndex(nextQueueIndex);
        return result;
    }

    public MessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    private void recover() {
        messageQueueHolder.recoverMessageQueue();
        commitLog.recover();
        messageQueueHolder.recoverTopicMQIndex();
    }


    /**
     * @param old      old index
     * @param newIndex new queue index for next
     * @return
     */
    private long nextQueueIndex(long old, long newIndex) {
        long next = newIndex;
        return next;
    }

    private boolean isEnough(GetMessageResult result, int maxMessageSize) {
        if (result.getMessageCount() >= maxMessageSize) {
            return true;
        }
        return false;
    }

}
