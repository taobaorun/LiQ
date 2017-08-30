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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/23 13:51
 */
public class DefaultCommitLogTest {

    private CommitLog defaultCommitLog;

    @Before
    public void setUp() throws Exception {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setCommitLogStorePath(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath() + "store");
        config.setCommitLogFileSize(1024);
        defaultCommitLog = new DefaultCommitLog(config);
        defaultCommitLog.load();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void putMessage() throws Exception {
        Message message = new Message();
        message.getMeta().setTopic("LiQTopic");
        for (int i = 0; i < 1000; i++) {
            message.setData(("Hello LiQ! " + i).getBytes());
            defaultCommitLog.putMessage(message);
        }
    }

    @Test
    public void getMessage() throws Exception {
        Message message = new Message();
        message.getMeta().setTopic("LiQTopic");
        TreeMap<Long, Integer> map = new TreeMap<>();
        for (int i = 0; i < 1000; i++) {
            message.setData(("Hello LiQ! " + i).getBytes());
            PutMessageResult result = defaultCommitLog.putMessage(message);
            if (result.getStatus() == PutMessageStatus.PUT_OK) {
                map.put(result.getAppendResult().getWroteOffset(), result.getAppendResult().getWroteBytes());
            }
        }
        int index = 0;
        for (Map.Entry<Long, Integer> entry : map.entrySet()) {
            SelectedMappedFileSection selectedMappedFileSection = defaultCommitLog.getMessage(entry.getKey(), entry.getValue());
            Assert.assertNotNull(selectedMappedFileSection);
            MessageProtocol protocol = new MessageProtocol();
            message = protocol.readMessage(selectedMappedFileSection.getByteBuffer());
            Assert.assertNotNull(message);
            Assert.assertEquals("Hello LiQ! " + (index++), new String(message.getData()));
        }
        Assert.assertEquals(1000,index);
    }


    @Test
    public void recover() throws Exception {
        defaultCommitLog.recover();
        Message message = new Message();
        message.getMeta().setTopic("LiQTopic");
        TreeMap<Long, Integer> map = new TreeMap<>();
        for (int i = 0; i < 10; i++) {
            message.setData(("Hello LiQ! " + i).getBytes());
            PutMessageResult result = defaultCommitLog.putMessage(message);
            Assert.assertEquals(PutMessageStatus.PUT_OK,result.getStatus());
            map.put(result.getAppendResult().getWroteOffset(), result.getAppendResult().getWroteBytes());
        }
    }

}