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

/**
 * Description: <br/>
 * <p>
 * store messages
 * <p>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/22 10:40
 */
public interface MessageStore {


    /**
     * load stored messages
     *
     * @return
     */
    boolean load();


    void start();


    /**
     * stop the message store service
     */
    void shutdown();


    /**
     * store the message
     *
     * @param message received message
     * @return {@link PutMessageResult}
     */
    PutMessageResult putMessage(Message message);


    /**
     * @param topic          topic
     * @param queueId        message queue id
     * @param queueIndex     message queue index
     * @param maxMessageSize get max message size
     * @return messages by the <code>topic</code> ,<code>queueId<code/> and the <code>queueIndex</code>
     */
    GetMessageResult getMessage(String topic, Integer queueId, long queueIndex, int maxMessageSize);

}
