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
        this.messageQueueHolder = new MessageQueueHolder(this.storeConfig);
        this.pipeline = new MessageEventPipeline(storeConfig, messageQueueHolder, 1000);
        this.commitLog = new DefaultCommitLog(this.storeConfig);
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


    public MessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    private void recover() {
        messageQueueHolder.recoverMessageQueue();
        commitLog.recover();
        recoverTopicMQIndex();
    }


    private void recoverTopicMQIndex() {
        if (commitLog instanceof DefaultCommitLog) {
            ((DefaultCommitLog) commitLog).resetTopicMQIndex(messageQueueHolder.newTopicMQIndex());
        }
    }


}
