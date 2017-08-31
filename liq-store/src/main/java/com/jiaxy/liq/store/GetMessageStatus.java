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
 * @Date: 2017/08/31 11:08
 */
public enum GetMessageStatus {

    //got messages
    FOUND,

    //no message or not matched
    NOT_FOUND,

    //message queue is empty
    NO_MESSAGE_IN_MQ,

    //message queue index small than the queue min index
    MQ_INDEX_TOO_SMALL,

    //message queue index greater than the queue MAX index
    MQ_INDEX_OVERFLOW,

    //message queue found ,but get null content
    MQ_SHOULD_NOT_NULL,

    //message queue not found by the topic and queueId
    MQ_NOT_FOUND;
}
