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
package io.airlift.compress.v3.internal;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMemorySegmentAccess
{
    @Test
    void testSharedArenaClosureDuringOperation()
    {
        Arena arena = Arena.ofShared();
        MemorySegment input = arena.allocate(8);
        input.set(JAVA_BYTE, 0, (byte) 42);
        MemorySegmentAccess access = MemorySegmentAccess.of(input, arena.allocate(8));
        arena.close();

        // The codec only accesses heap copies while a concurrently closed arena is unavailable.
        assertThat(access.input().get(JAVA_BYTE, 0)).isEqualTo((byte) 42);
        access.output().set(JAVA_BYTE, 0, (byte) 7);
        assertThatThrownBy(() -> access.finish(1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testConfinedAccessDoesNotCopy()
    {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment input = arena.allocate(8);
            MemorySegment output = arena.allocate(8);
            MemorySegmentAccess access = MemorySegmentAccess.of(input, output);
            assertThat(access.input()).isSameAs(input);
            assertThat(access.output()).isSameAs(output);
        }
    }

    @Test
    void testOnlyProducedOutputIsCopied()
    {
        try (Arena arena = Arena.ofShared()) {
            MemorySegment output = arena.allocate(8).fill((byte) 11);
            MemorySegmentAccess access = MemorySegmentAccess.of(MemorySegment.ofArray(new byte[0]), output);
            access.output().set(JAVA_BYTE, 0, (byte) 7);
            assertThat(access.finish(1)).isEqualTo(1);
            assertThat(output.get(JAVA_BYTE, 0)).isEqualTo((byte) 7);
            assertThat(output.get(JAVA_BYTE, 1)).isEqualTo((byte) 11);
        }
    }
}
