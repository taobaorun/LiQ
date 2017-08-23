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
 * @Date: 2017/08/22 15:43
 */
public class AppendMeta {

    //the position the message wrote
    private long wroteOffset;

    private int wroteBytes;

    private String msgId;

    private long queueOffset;

    private long storeTimestamp;

    private int pageCacheRT;

    public AppendMeta(long wroteOffset, int wroteBytes, String msgId, long queueOffset, long storeTimestamp, int pageCacheRT) {
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.queueOffset = queueOffset;
        this.storeTimestamp = storeTimestamp;
        this.pageCacheRT = pageCacheRT;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public int getPageCacheRT() {
        return pageCacheRT;
    }

    public void setPageCacheRT(int pageCacheRT) {
        this.pageCacheRT = pageCacheRT;
    }
}
