/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

class GaussianThroughputGeneratorTest {

    private static Object get(GaussianThroughputGenerator gen, String name) throws Exception {
        Field f = GaussianThroughputGenerator.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(gen);
    }

    private static void set(GaussianThroughputGenerator gen, String name, Object value) throws Exception {
        Field f = GaussianThroughputGenerator.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(gen, value);
    }

    private static void callCalc(GaussianThroughputGenerator gen, boolean force) throws Exception {
        Method m = GaussianThroughputGenerator.class.getDeclaredMethod("calculateNextWindow", boolean.class);
        m.setAccessible(true);
        m.invoke(gen, force);
    }

    @Test
    void ctorAndAccessors() {
        // windowSizeMs <= 0 should default to 100 (per implementation)
        GaussianThroughputGenerator gen =
                new GaussianThroughputGenerator(50, 5.0, 3, 0);

        assertEquals(50, gen.messagesPerWindowAverage());
        assertEquals(5.0, gen.messagesPerWindowDeviation(), 0.000001);
        assertEquals(3, gen.windowsUntilRateChange());
        assertEquals(100L, gen.windowSizeMs());
    }

    @Test
    void advancesWhenNextWindowEqualsNow() throws Exception {
        GaussianThroughputGenerator gen =
                new GaussianThroughputGenerator(10, /*dev*/ 0.0, /*rateChange*/ 2, /*window*/ 25);

        long now = Time.SYSTEM.milliseconds();
        // Force nextWindowStarts to equal 'now' to exercise the while (<= now) logic
        set(gen, "nextWindowStarts", now);

        callCalc(gen, /*force=*/false);

        long next = (long) get(gen, "nextWindowStarts");
        // With window size 25ms and starting at 'now', the next start must be exactly now + 25
        assertEquals(now + 25L, next, "nextWindowStarts should advance by exactly one window when equal to now");
    }

    @Test
    void triggersRateChangeOnEquality() throws Exception {
        // Use deviation = 0 so throttleMessages becomes exactly 'average' deterministically.
        GaussianThroughputGenerator gen =
                new GaussianThroughputGenerator(/*avg*/ 17, /*dev*/ 0.0, /*windowsUntilRateChange*/ 3, /*window*/ 20);

        // Put us at the equality boundary: windowTracker == windowsUntilRateChange
        set(gen, "windowTracker", 3);

        // Also set nextWindowStarts so calculateNextWindow doesn't loop forever
        set(gen, "nextWindowStarts", Time.SYSTEM.milliseconds());

        callCalc(gen, /*force=*/false);

        int windowTracker = (int) get(gen, "windowTracker");
        int throttleMessages = (int) get(gen, "throttleMessages");

        // After equality trigger, counter is reset to 0 and then incremented to 1
        assertEquals(1, windowTracker, "windowTracker should reset on equality and then increment to 1");
        // With deviation 0, the chosen throttle must equal the average and be >= 1
        assertEquals(17, throttleMessages, "throttleMessages should equal average when deviation is zero");
    }

    @Test
    void throttleAdvancesWindowWithoutWaiting() throws Exception {
        GaussianThroughputGenerator gen =
                new GaussianThroughputGenerator(/*avg*/ 10, /*dev*/ 1.0, /*rateChange*/ 2, /*window*/ 5);

        // Make it look like the current window already ended so throttle() will immediately recalc
        long now = Time.SYSTEM.milliseconds();
        set(gen, "nextWindowStarts", now - 1);

        // Avoid entering the 'wait(...)' loop: keep messageTracker < throttleMessages
        set(gen, "throttleMessages", Integer.MAX_VALUE);

        long before = (long) get(gen, "nextWindowStarts");
        gen.throttle(); // should not block

        long after = (long) get(gen, "nextWindowStarts");
        int messageTracker = (int) get(gen, "messageTracker");

        assertTrue(after >= before + gen.windowSizeMs(),
                "throttle() should schedule a new window when we are past the current one");
        assertEquals(1, messageTracker, "messageTracker should increment by 1 for each throttle call");
    }
}
