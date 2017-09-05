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
 * @Date: 2017/08/21 14:34
 */
public class PutMessageResult implements Comparable<PutMessageResult> {

    private PutMessageStatus status;

    private AppendMeta appendResult;


    public PutMessageResult(PutMessageStatus status) {
        this.status = status;
    }

    public PutMessageResult() {
    }

    public PutMessageStatus getStatus() {
        return status;
    }

    public void setStatus(PutMessageStatus status) {
        this.status = status;
    }

    public AppendMeta getAppendResult() {
        return appendResult;
    }

    public void setAppendResult(AppendMeta appendResult) {
        this.appendResult = appendResult;
    }


    @Override
    public int compareTo(PutMessageResult o) {
        if (getAppendResult().getWroteOffset() < o.getAppendResult().getWroteOffset()) {
            return -1;
        } else {
            return 1;
        }
    }

    @Override
    public String toString() {
        return "PutMessageResult{" +
                "status=" + status +
                ", appendResult=" + appendResult +
                '}';
    }
}


