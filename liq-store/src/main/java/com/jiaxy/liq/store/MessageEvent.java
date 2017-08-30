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

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/29 17:49
 */
public class MessageEvent implements Comparable<MessageEvent> {

    private String topic;

    private Integer queueId;

    private long tagsCode;

    private PutMessageResult putRs;

    public MessageEvent(String topic, Integer queueId, long tagsCode, PutMessageResult putRs) {
        this.topic = topic;
        this.queueId = queueId;
        this.tagsCode = tagsCode;
        this.putRs = putRs;
    }

    @Override
    public int compareTo(MessageEvent o) {
        if (this.getPutRs().getAppendResult().getWroteOffset() < o.getPutRs().getAppendResult().getWroteOffset()) {
            return -1;
        }
        return 1;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }

    public PutMessageResult getPutRs() {
        return putRs;
    }

    public void setPutRs(PutMessageResult putRs) {
        this.putRs = putRs;
    }
}
