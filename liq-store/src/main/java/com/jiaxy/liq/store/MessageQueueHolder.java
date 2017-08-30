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
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/29 14:53
 */
public class MessageQueueHolder {

    private static final Logger logger = LoggerFactory.getLogger(MessageQueueHolder.class);

    private final ConcurrentHashMap<String /*topic*/, ConcurrentHashMap<Integer /*queueId*/, MessageQueueValue>> mqMap;

    private final MessageStoreConfig storeConfig;

    public MessageQueueHolder(MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        mqMap = new ConcurrentHashMap<>(32);
    }


    public boolean load() {
        if (!checkMQDir()) {
            return false;
        }
        File mqStore = new File(storeConfig.getMessageQueueStorePath());
        if (!mqStore.isDirectory()) {
            logger.error("{} should be a directory", storeConfig.getMessageQueueStorePath());
            return false;
        }
        File[] topicFiles = mqStore.listFiles();
        if (topicFiles != null && topicFiles.length > 0) {
            for (File topicFile : topicFiles) {
                String topic = topicFile.getName();
                File[] mqFiles = topicFile.listFiles();
                for (File mqFile : mqFiles) {
                    String fileName = mqFile.getName();
                    Integer queueId = Integer.valueOf(fileName);
                    MessageQueueValue value = new MessageQueueValue();
                    value.setMessageQueue(newMessageQueue(topic, queueId, storeConfig));
                    putMessageQueue(topic, queueId, value);
                    if (!value.getMessageQueue().load()) {
                        logger.error("message queue load failed.Topic:{},QueueId:{}", topic, queueId);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public void recoverMessageQueue() {
        for (ConcurrentHashMap<Integer, MessageQueueValue> mqs : mqMap.values()) {
            for (MessageQueueValue v : mqs.values()) {
                if (v.getMessageQueue() != null) {
                    v.getMessageQueue().recover();
                }
            }
        }
    }

    public HashMap<String, Long> newTopicMQIndex() {
        HashMap<String, Long> map = new HashMap<>();
        for (ConcurrentHashMap<Integer, MessageQueueValue> mqs : mqMap.values()) {
            for (MessageQueueValue v : mqs.values()) {
                MessageQueue mq = v.getMessageQueue();
                if (mq != null) {
                    map.put(DefaultCommitLog.topicQueueKey(mq.getTopic(), mq.getQueueId()), mq.getMessageQueueMaxIndex());
                }
            }
        }
        return map;
    }


    /**
     * @param topic
     * @param queueId
     * @return {@link MessageQueue} or new one
     */
    public MessageQueue getMessageQueue(String topic, Integer queueId) {
        ConcurrentHashMap<Integer, MessageQueueValue> topicMQ = mqMap.get(topic);
        if (topicMQ == null) {
            ConcurrentHashMap<Integer, MessageQueueValue> newTopicMQ = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, MessageQueueValue> oldTopicMQ = mqMap.putIfAbsent(topic, newTopicMQ);
            if (oldTopicMQ != null) {
                topicMQ = oldTopicMQ;
            } else {
                topicMQ = newTopicMQ;
            }
        }
        MessageQueueValue value = null;
        MessageQueueValue oldValue = topicMQ.putIfAbsent(queueId, new MessageQueueValue());
        if (oldValue != null) {
            value = oldValue;
        } else {
            value = topicMQ.get(queueId);
            topicMQ.get(queueId);
        }
        if (value.getMessageQueue() == null) {
            synchronized (value) {
                if (value.getMessageQueue() == null) {
                    value.setMessageQueue(newMessageQueue(topic,
                            queueId,
                            storeConfig));
                }
            }
        }
        return value.getMessageQueue();
    }


    private MessageQueue newMessageQueue(String topic, Integer queueId, MessageStoreConfig storeConfig) {
        MessageQueue messageQueue = new MessageQueue(topic, queueId, storeConfig);
        return messageQueue;
    }

    private boolean checkMQDir() {
        try {
            return FileUtil.checkDirectory(storeConfig.getMessageQueueStorePath(), true);
        } catch (Exception e) {
            logger.error("make the message queue dir:{} error.", storeConfig.getMessageQueueStorePath(), e);
            return false;
        }
    }

    private void putMessageQueue(String topic, Integer queueId, MessageQueueValue value) {
        if (!mqMap.containsKey(topic)) {
            mqMap.put(topic, new ConcurrentHashMap<>());
        }
        ConcurrentHashMap<Integer, MessageQueueValue> queueMap = mqMap.get(topic);
        if (!queueMap.containsKey(queueId)) {
            queueMap.put(queueId, value);
        }
    }


    private class MessageQueueValue {

        private MessageQueue messageQueue;

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public void setMessageQueue(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }
    }
}
