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

import org.junit.Assert;
import org.junit.Test;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/21 18:30
 */
public class MappedFileQueueTest {


    @Test
    public void getMappedFile() throws Exception {
        MappedFileQueue mappedFileQueue = new MappedFileQueue(Thread.currentThread()
                .getContextClassLoader()
                .getResource("")
                .getPath(), 1024);
        MappedFile mappedFile = mappedFileQueue.getMappedFile(100, true);
        Assert.assertNotNull(mappedFile);
        mappedFile.appendData("123".getBytes());
        MappedFile mappedFile2 = mappedFileQueue.getMappedFile(200, true);
        Assert.assertEquals(mappedFile, mappedFile2);
        for (int i = 0; i < 1021; i++) {
            mappedFile2.appendData(String.valueOf(1).getBytes());
        }
        Assert.assertEquals(true, mappedFile2.isFull());
        MappedFile mappedFile3 = mappedFileQueue.getMappedFile(200, true);
        Assert.assertNotEquals(mappedFile, mappedFile3);
        for (int i = 0; i < 1024; i++) {
            mappedFile3.appendData(String.valueOf(1).getBytes());
        }
        Assert.assertEquals(true, mappedFile3.isFull());
    }

}