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
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/21 14:32
 */
public interface CommitLog {


    PutMessageResult putMessage(Message message);


    /**
     * @param phyOffset
     * @param size      message size
     * @return
     */
    SelectedMappedFileSection getMessage(long phyOffset, int size);


    void flush();


    /**
     * load the store dir files </br>
     * <p>
     * just load the file.after {@link #recover()} successfully </br>
     * <p>
     * the commit log will be ready for operations (eg.{@link #putMessage(Message)})
     *
     * @return
     */
    boolean load();


    /**
     * commit log recover
     */
    void recover();


}
