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

    private final DefaultCommitLog commitLog;

    public MessageQueueHolder(MessageStoreConfig storeConfig, DefaultCommitLog commitLog) {
        this.commitLog = commitLog;
        this.storeConfig = storeConfig;
        mqMap = new ConcurrentHashMap<>(32);
    }


    public boolean load() {
        if (!checkMQRootDir()) {
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

    /**
     * recover topic message queue
     */
    public void recoverMessageQueue() {
        for (ConcurrentHashMap<Integer, MessageQueueValue> mqs : mqMap.values()) {
            for (MessageQueueValue v : mqs.values()) {
                if (v.getMessageQueue() != null) {
                    v.getMessageQueue().recover();
                }
            }
        }
    }

    /**
     * recover queue index of each topic message queue
     */
    public void recoverTopicMQIndex() {
        HashMap<String, Long> map = new HashMap<>();
        long commitLogMinOffset = commitLog.getMinOffset();
        for (ConcurrentHashMap<Integer, MessageQueueValue> mqs : mqMap.values()) {
            for (MessageQueueValue v : mqs.values()) {
                MessageQueue mq = v.getMessageQueue();
                if (mq != null) {
                    map.put(DefaultCommitLog.topicQueueKey(mq.getTopic(), mq.getQueueId()), mq.getMessageQueueMaxIndex());
                    //update message queue min offset
                    mq.updateMinMessageQueueOffset(commitLogMinOffset);

                }
            }
        }
        commitLog.resetTopicMQIndex(map);
    }


    /**
     * get message queue or create message queue if not found
     *
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

    /**
     * find message queue by topic and queueId
     *
     * @param topic
     * @param queueId
     * @return
     */
    public MessageQueue findMessageQueue(String topic, Integer queueId) {
        ConcurrentHashMap<Integer, MessageQueueValue> topicMQ = mqMap.get(topic);
        if (topicMQ == null) {
            return null;
        }
        MessageQueueValue v = topicMQ.get(queueId);
        if (v != null) {
            return v.getMessageQueue();
        }
        return null;
    }


    private MessageQueue newMessageQueue(String topic, Integer queueId, MessageStoreConfig storeConfig) {
        if (!checkMessageQueueDir(topic, queueId)) {
            logger.warn("");
        }
        MessageQueue messageQueue = new MessageQueue(topic, queueId, storeConfig);
        return messageQueue;
    }

    private boolean checkMQRootDir() {
        try {
            return FileUtil.checkDirectory(storeConfig.getMessageQueueStorePath(), true);
        } catch (Exception e) {
            logger.error("make the message queue root dir:{} error.", storeConfig.getMessageQueueStorePath(), e);
            return false;
        }
    }

    private boolean checkMessageQueueDir(String topic, Integer queueId) {
        String queueDir = storeConfig.getMessageQueueStorePath() + File.separator + topic + File.separator + queueId;
        try {
            return FileUtil.checkDirectory(queueDir, true);
        } catch (Exception e) {
            logger.error("make the message queue dir:{} error.", queueDir, e);
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
