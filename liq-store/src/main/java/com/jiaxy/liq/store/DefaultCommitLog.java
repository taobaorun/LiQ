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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/21 15:12
 */
public class DefaultCommitLog implements CommitLog {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommitLog.class);

    public PutMessageResult putMessage(Message message) {
        return null;
    }

    public boolean load() {
        return false;
    }

    public void recover() {

    }
}
