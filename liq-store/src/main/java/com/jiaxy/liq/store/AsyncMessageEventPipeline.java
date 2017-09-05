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

import com.jiaxy.liq.common.ServiceThread;
import com.jiaxy.liq.core.message.MessageMeta;

import java.util.ArrayList;
import java.util.List;

import static com.jiaxy.liq.core.message.MessageProtocol.readMessageMeta;
import static com.jiaxy.liq.store.AppendMeta.AppendStatus.APPEND_OK;
import static com.jiaxy.liq.store.PutMessageStatus.PUT_OK;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/09/04 11:39
 */
public class AsyncMessageEventPipeline implements MessageEventPipeline {

    private MessageQueuePutService putService = new MessageQueuePutService();

    private final List<MessageEventHandler> handlers = new ArrayList<>();

    private final CommitLog commitLog;

    private final MessageStoreConfig storeConfig;

    private final MessageQueueHolder messageQueueHolder;

    public AsyncMessageEventPipeline(MessageStoreConfig storeConfig, MessageQueueHolder messageQueueHolder, CommitLog commitLog) {
        this.commitLog = commitLog;
        this.storeConfig = storeConfig;
        this.messageQueueHolder = messageQueueHolder;
        handlers.add(new MessageQueueHandler(this.storeConfig.getPutMQRetryTime(), this.messageQueueHolder));
    }

    @Override
    public boolean messagePut(MessageEvent event) {
        if (event.getPutRs().getStatus() == PUT_OK) {
            putService.wakeup();
        }
        return true;
    }

    @Override
    public void dispatch() {

    }


    @Override
    public void dispatch(MessageEvent event) {
        for (MessageEventHandler handler : handlers) {
            handler.handle(event);
        }
    }


    @Override
    public void start() {
        putService.setMqPutOffset(this.commitLog.getMaxOffset());
        putService.start();
    }

    private class MessageQueuePutService extends ServiceThread {
        //message queue has handled the offset of commit log
        private long mqPutOffset;

        @Override
        public String serviceName() {
            return "MQ-PUT";
        }

        @Override
        public void run() {
            while (true) {
                putService.waitForRunning(1000);
                doMQPut();
            }

        }

        public void setMqPutOffset(long mqPutOffset) {
            this.mqPutOffset = mqPutOffset;
        }

        private void doMQPut() {
            while (available()) {
                SelectedMappedFileSection section = commitLog.getMessage(mqPutOffset);
                mqPutOffset = section.getStartOffset();
                for (int readSize = 0; readSize < section.getSize(); ) {
                    MessageMeta messageMeta = readMessageMeta(section.getByteBuffer(), true);
                    if (messageMeta.isPadding()) {
                        mqPutOffset = commitLog.rollNextFile(mqPutOffset);
                        break;
                    } else {
                        readSize += messageMeta.getTotalLength();
                        mqPutOffset += messageMeta.getTotalLength();
                        PutMessageResult putMessageResult = new PutMessageResult(PUT_OK);
                        AppendMeta appendMeta = new AppendMeta(APPEND_OK, messageMeta.getCommitLogOffset(),
                                messageMeta.getTotalLength(),
                                messageMeta.getMsgId(),
                                messageMeta.getQueueOffset(),
                                messageMeta.getStoredTimestamp(),
                                0);
                        putMessageResult.setAppendResult(appendMeta);
                        MessageEvent event = new MessageEvent(
                                messageMeta.getTopic(),
                                messageMeta.getQueueId(),
                                messageMeta.getTags().hashCode(),
                                putMessageResult
                        );
                        dispatch(event);
                    }
                }
            }
        }


        private boolean available() {
            return mqPutOffset < commitLog.getMaxOffset();
        }
    }
}

