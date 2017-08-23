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

package com.jiaxy.liq.common;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/08/22 18:56
 */
public class StringUtilTest {
    @Test
    public void bytesToHex() throws Exception {
        System.out.println(StringUtil.bytesToHex("COLE".getBytes()));
        System.out.println(StringUtil.bytesToHex("BABA".getBytes()));
        System.out.println(0x434f4c45 & 0x42414241);
        System.out.println(0xBBCCDDEE ^ 1880681586 + 8);
    }

}