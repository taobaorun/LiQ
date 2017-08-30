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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/28 14:25
 */
public class MessageQueueHandler implements MessageEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(MessageQueueHandler.class);

    private final MessageQueueHolder mqHolder;

    private final int retryTime;


    public MessageQueueHandler(int retryTime, MessageQueueHolder messageQueueHolder) {
        this.retryTime = retryTime;
        this.mqHolder = messageQueueHolder;
    }


    @Override
    public boolean handle(MessageEvent event) {
        boolean result = false;
        for (int i = 0; !result && i <= retryTime; i++) {
            MessageQueue messageQueue = mqHolder.getMessageQueue(event.getTopic(), event.getQueueId());
            if (messageQueue == null) {
                result = false;
                continue;
            }
            AppendMeta appendMeta = event.getPutRs().getAppendResult();
            result = messageQueue.putMessageQueue(
                    appendMeta.getWroteOffset(),
                    appendMeta.getWroteBytes(),
                    event.getTagsCode(),
                    appendMeta.getQueueOffset()
            );
        }
        if (!result) {
            logger.warn("message queue handle failed,retried:{} .Topic:{},QueueId:{}", retryTime, event.getTopic(), event.getQueueId());
        }
        return result;
    }
}
