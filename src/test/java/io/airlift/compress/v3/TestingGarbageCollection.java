/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.v3;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public final class TestingGarbageCollection
{
    private TestingGarbageCollection() {}

    public static void assertCollected(Reference<?> reference)
            throws InterruptedException
    {
        assumeFalse(Boolean.parseBoolean(ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).getVMOption("DisableExplicitGC").getValue()), "Explicit GC is disabled");
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!reference.refersTo(null) && System.nanoTime() < deadline) {
            System.gc();
            Thread.sleep(50);
        }
        assertThat(reference.refersTo(null)).isTrue();
    }
}
