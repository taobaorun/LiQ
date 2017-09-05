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

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/09/04 16:02
 */
public class ServiceThreadTest {

    ServiceThreadPrint print = new ServiceThreadPrint();


    @Test
    public void wakeup() throws Exception {
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(10000);
                        print.wakeup();
                    } catch (Exception e) {
                    }

                }
            }
        }).start();
        print.start();
        TimeUnit.SECONDS.sleep(1000);

    }

    class ServiceThreadPrint extends ServiceThread {

        public String serviceName() {
            return "test";
        }

        public void run() {
            while (true) {
                waitForRunning(1000);
                System.out.println("--" + new Date());
            }

        }
    }

}