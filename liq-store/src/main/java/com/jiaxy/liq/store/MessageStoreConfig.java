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
 * @Date: 2017/08/22 11:11
 */
public class MessageStoreConfig {

    //message stored dir path
    private String commitLogStorePath;

    private String messageQueueStorePath;

    //1G
    private int commitLogFileSize = 1024 * 1024 * 1024;

    //message queue file size
    private int messageQueueFileSize = 1000 * 1000 * 6;

    //recover normally by the last files
    private int recoverBaseOnLastFileNum = 1;

    //put message queue retry time
    private int putMQRetryTime = 10;

    public String getCommitLogStorePath() {
        return commitLogStorePath;
    }

    public void setCommitLogStorePath(String commitLogStorePath) {
        this.commitLogStorePath = commitLogStorePath;
    }

    public String getMessageQueueStorePath() {
        return messageQueueStorePath;
    }

    public void setMessageQueueStorePath(String messageQueueStorePath) {
        this.messageQueueStorePath = messageQueueStorePath;
    }

    public int getCommitLogFileSize() {
        return commitLogFileSize;
    }

    public void setCommitLogFileSize(int commitLogFileSize) {
        this.commitLogFileSize = commitLogFileSize;
    }

    public int getRecoverBaseOnLastFileNum() {
        return recoverBaseOnLastFileNum;
    }

    public int getMessageQueueFileSize() {
        return messageQueueFileSize;
    }

    public void setMessageQueueFileSize(int messageQueueFileSize) {
        this.messageQueueFileSize = messageQueueFileSize;
    }

    public int getPutMQRetryTime() {
        return putMQRetryTime;
    }

    public void setRecoverBaseOnLastFileNum(int recoverBaseOnLastFileNum) {
        this.recoverBaseOnLastFileNum = recoverBaseOnLastFileNum;
    }
}
