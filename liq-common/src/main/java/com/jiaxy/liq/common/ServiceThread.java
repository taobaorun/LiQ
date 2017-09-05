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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Description: <br/>
 * <p/>
 * <br/>
 *
 * @Date: 2017/09/04 11:50
 */
public abstract class ServiceThread implements Runnable {

    private final Thread serviceThread;

    protected volatile AtomicBoolean awaked = new AtomicBoolean(false);

    private final CyclicBarrier cb = new CyclicBarrier(2);

    public ServiceThread() {
        serviceThread = new Thread(this, "LiQ-ST-" + serviceName());
    }


    public void start() {
        serviceThread.start();
    }


    public void stop() {

    }

    public void shutdown() {

    }

    public void wakeup() {
        if (awaked.compareAndSet(false, true)) {
            try {
                if (!cb.isBroken()) {
                    cb.await();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    protected void waitForRunning(long timeout) {
        if (awaked.compareAndSet(true, false)) {
            return;
        }
        try {
            cb.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            cb.reset();
        }

    }


    public abstract String serviceName();
}
