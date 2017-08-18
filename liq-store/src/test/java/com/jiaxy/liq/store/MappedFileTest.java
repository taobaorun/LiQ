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

import com.jiaxy.liq.common.FileUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/18 16:17
 */
public class MappedFileTest {

    private MappedFile mmFile;

    @Before
    public void setUp() throws Exception {
        mmFile = new MappedFile(FileUtil.getFileName(0),2048);
    }

    @Test
    public void appendData() throws Exception {
        mmFile.appendData("123\n".getBytes());
        mmFile.appendData("456\n".getBytes());
        mmFile.appendData("789\n".getBytes());
    }


    @Test
    public void destroy() throws Exception {
        Assert.assertNotNull(mmFile);
        mmFile.destroy();
    }

    @Test
    public void selectMappedFileSection() throws Exception {
        mmFile.appendData("123\n".getBytes());
        mmFile.appendData("456\n".getBytes());
        mmFile.appendData("789\n".getBytes());
        SelectedMappedFileSection section = mmFile.selectMappedFileSection(0, 3);
        Assert.assertNotNull(section);
        byte[] data = new byte[section.getSize()];
        section.getByteBuffer().get(data);
        Assert.assertEquals("123",new String(data));
        section = mmFile.selectMappedFileSection(1, 3);
        Assert.assertNotNull(section);
        section.getByteBuffer().get(data);
        Assert.assertEquals("23\n",new String(data));
        section = mmFile.selectMappedFileSection(8, 3);
        Assert.assertNotNull(section);
        section.getByteBuffer().get(data);
        Assert.assertEquals("789",new String(data));
    }
}