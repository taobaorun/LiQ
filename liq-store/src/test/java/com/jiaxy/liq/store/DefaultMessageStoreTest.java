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
import com.jiaxy.liq.core.message.MessageProtocol;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/29 18:10
 */
public class DefaultMessageStoreTest {

    private MessageStoreConfig storeConfig;

    @Before
    public void setUp() throws Exception {
        storeConfig = new MessageStoreConfig();
        storeConfig.setCommitLogStorePath(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath() + "store/commitlog");
        storeConfig.setCommitLogFileSize(1024);
        storeConfig.setMessageQueueStorePath(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath() + "store/mq");
        storeConfig.setMessageQueueFileSize(1000);

    }

    @Test
    public void putMessage() throws Exception {
        MessageStore defaultMessageStore = new DefaultMessageStore(storeConfig);
        defaultMessageStore.load();
        defaultMessageStore.start();
        Message message = new Message();
        message.getMeta().setTopic("LiQTopic");
        for (int i = 0; i < 1000; i++) {
            message.setData(("Hello LiQ! " + i).getBytes());
            defaultMessageStore.putMessage(message);
        }
        TimeUnit.SECONDS.sleep(1);
    }


    @Test
    public void getMessage() throws Exception {
        resetStoreDir();
        MessageStore defaultMessageStore = new DefaultMessageStore(storeConfig);
        defaultMessageStore.load();
        defaultMessageStore.start();
        //put messages
        Message message = new Message();
        message.getMeta().setTopic("LiQTopic");
        for (int i = 0; i < 1000; i++) {
            message.setData(("Hello LiQ! " + i).getBytes());
            defaultMessageStore.putMessage(message);
        }
        TimeUnit.SECONDS.sleep(1);
        //get messages
        int messageCount = 0;
        long queueIndex = 0;
        GetMessageResult result = defaultMessageStore.getMessage("LiQTopic", 0, 1000, 100);
        Assert.assertEquals(GetMessageStatus.MQ_INDEX_OVERFLOW, result.getStatus());
        result = defaultMessageStore.getMessage("LiQTopic", 0, 999, 100);
        Assert.assertEquals(1, result.getMessageCount());
        do {
            result = defaultMessageStore.getMessage("LiQTopic", 0, queueIndex, 100 - messageCount);
            messageCount += result.getMessageCount();
            queueIndex = result.getNextQueueIndex();
            printMessage(result);

        } while (messageCount < 100);

    }


    private void printMessage(GetMessageResult result) {
        MessageProtocol protocol = new MessageProtocol();
        for (int i = 0; i < result.getMessageCount(); i++) {
            Message message = protocol.readMessage(result.getSelectedSections().get(i).getByteBuffer());
            System.out.println(new String(message.getData()));
        }
    }

    private String timestampDir() {
        LocalDateTime ldt = LocalDateTime.now();
        return ldt.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
    }

    private void resetStoreDir() {
        String timestampDir = timestampDir();
        storeConfig.setCommitLogStorePath(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath() + "store/" + timestampDir + "/commitlog");
        storeConfig.setMessageQueueStorePath(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath() + "store/" + timestampDir + "/mq");

    }

}