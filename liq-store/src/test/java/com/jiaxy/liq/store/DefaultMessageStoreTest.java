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
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/29 18:10
 */
public class DefaultMessageStoreTest {

    private DefaultMessageStore defaultMessageStore;

    @Before
    public void setUp() throws Exception {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setCommitLogStorePath(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath() + "store");
        storeConfig.setCommitLogFileSize(1024);
        storeConfig.setMessageQueueStorePath(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath() + "mq");
        storeConfig.setMessageQueueFileSize(1000);
        defaultMessageStore = new DefaultMessageStore(storeConfig);
        defaultMessageStore.load();
        defaultMessageStore.start();
    }

    @Test
    public void putMessage() throws Exception {
        Message message = new Message();
        message.getMeta().setTopic("LiQTopic");
        for (int i = 0; i < 1000; i++) {
            message.setData(("Hello LiQ! " + i).getBytes());
            defaultMessageStore.putMessage(message);
        }
        TimeUnit.SECONDS.sleep(1);

    }

}