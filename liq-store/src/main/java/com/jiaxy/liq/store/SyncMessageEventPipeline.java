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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Description: <br/>
 * message pipeline
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/28 10:24
 */
public class SyncMessageEventPipeline implements MessageEventPipeline {

    private final PriorityBlockingQueue<MessageEvent> pipeline;

    private final List<MessageEventHandler> handlers = new ArrayList<>();

    private final MessageQueueHolder messageQueueHolder;

    private final MessageStoreConfig storeConfig;

    private final Thread thread;


    public SyncMessageEventPipeline(MessageStoreConfig storeConfig, MessageQueueHolder messageQueueHolder, int capacity) {
        this.storeConfig = storeConfig;
        this.messageQueueHolder = messageQueueHolder;
        pipeline = new PriorityBlockingQueue<>(capacity);
        handlers.add(new MessageQueueHandler(this.storeConfig.getPutMQRetryTime(), this.messageQueueHolder));
        thread = new Thread(() -> dispatch(), "message-event-pipeline");
        thread.setDaemon(true);
    }

    public boolean messagePut(MessageEvent event) {
        pipeline.put(event);
        return true;
    }


    public void start() {
        thread.start();
    }


    public void dispatch() {
        while (true) {
            try {
                MessageEvent event = pipeline.take();
                for (MessageEventHandler handler : handlers) {
                    if (!handler.handle(event)) {
                        pipeline.put(event);
                        TimeUnit.MILLISECONDS.sleep(1);
                    }
                }
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void dispatch(MessageEvent event) {

    }
}
