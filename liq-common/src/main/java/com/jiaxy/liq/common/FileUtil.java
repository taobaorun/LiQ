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
 *    limitations under the License
 */

package com.jiaxy.liq.common;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.text.NumberFormat;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/18 15:29
 */
public class FileUtil {

    /**
     * get file name by offset
     *
     * @param offset file start offset
     * @return
     */
    public static String getFileName(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * unmap
     *
     * @param mappedByteBuffer
     */
    public static void unmap(MappedByteBuffer mappedByteBuffer) {
        //TODO

    }

    public static boolean checkDirectory(String dirPath, boolean needCreate) throws Exception {
        File dir = new File(dirPath);
        if (dir.exists()) {
            return true;
        } else if (needCreate) {
            try {
                return dir.mkdirs();
            } catch (Exception e) {
                throw e;
            }
        }
        return false;
    }
}
