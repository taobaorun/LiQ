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
        config.setStorePath(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath()+"store");
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
        message.setData("Hello LiQ".getBytes());
        defaultCommitLog.putMessage(message);
        defaultCommitLog.putMessage(message);
        defaultCommitLog.putMessage(message);
        defaultCommitLog.putMessage(message);
        defaultCommitLog.putMessage(message);
    }

    @Test
    public void getMessage() throws Exception {
        SelectedMappedFileSection selectedMappedFileSection = defaultCommitLog.getMessage(0, 54);
        Assert.assertNotNull(selectedMappedFileSection);
        MessageProtocol protocol = new MessageProtocol();
        Message message = protocol.readMessage(selectedMappedFileSection.getByteBuffer());
        Assert.assertNotNull(message);
        System.out.println(new String(message.getData()));
        selectedMappedFileSection = defaultCommitLog.getMessage(54, 54);
        message = protocol.readMessage(selectedMappedFileSection.getByteBuffer());
        System.out.println(new String(message.getData()));
        selectedMappedFileSection = defaultCommitLog.getMessage(108, 54);
        message = protocol.readMessage(selectedMappedFileSection.getByteBuffer());
        System.out.println(new String(message.getData()));
        selectedMappedFileSection = defaultCommitLog.getMessage(162, 54);
        message = protocol.readMessage(selectedMappedFileSection.getByteBuffer());
        System.out.println(new String(message.getData()));




    }

    @Test
    public void load() throws Exception {
    }

    @Test
    public void recover() throws Exception {
    }

}