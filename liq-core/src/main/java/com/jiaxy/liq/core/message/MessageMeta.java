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

package com.jiaxy.liq.core.message;

import java.net.SocketAddress;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/21 14:22
 */
public class MessageMeta {

    private String topic;

    private byte[] topicData;

    private int flag;

    private String msgId;

    private int msgType;

    // message total length
    private int totalLength;

    private int magicCode;

    private int queueId;

    private long queueOffset;

    private long commitLogOffset;

    //the address the message created
    private SocketAddress bornHost;

    private long bornTimestamp;

    //the address the message stored
    private SocketAddress storedHost;

    private long storedTimestamp;


    public boolean isPadding() {
        if (msgType == MessageType.PADDING) {
            return true;
        }
        return false;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public int getMsgType() {
        return msgType;
    }

    public void setMsgType(int msgType) {
        this.msgType = msgType;
    }

    public int getTotalLength() {
        return totalLength;
    }

    public void setTotalLength(int totalLength) {
        this.totalLength = totalLength;
    }

    public int getMagicCode() {
        return magicCode;
    }

    public void setMagicCode(int magicCode) {
        this.magicCode = magicCode;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public SocketAddress getBornHost() {
        return bornHost;
    }

    public void setBornHost(SocketAddress bornHost) {
        this.bornHost = bornHost;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public SocketAddress getStoredHost() {
        return storedHost;
    }

    public void setStoredHost(SocketAddress storedHost) {
        this.storedHost = storedHost;
    }

    public long getStoredTimestamp() {
        return storedTimestamp;
    }

    public void setStoredTimestamp(long storedTimestamp) {
        this.storedTimestamp = storedTimestamp;
    }

    public byte[] getTopicData() {
        return topicData;
    }

    public void setTopicData(byte[] topicData) {
        this.topicData = topicData;
    }
}
