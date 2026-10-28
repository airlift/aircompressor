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
import java.util.Arrays;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestNativeBuffers
{
    @Test
    void testHeapInputAndPartialOutput()
    {
        byte[] input = {1, 2, 3, 4};
        byte[] output = new byte[10];
        Arrays.fill(output, (byte) 42);
        try (NativeBuffers buffers = new NativeBuffers()) {
            MemorySegment source = buffers.input(MemorySegment.ofArray(input).asReadOnly(), input.length);
            assertThat(source.isNative()).isTrue();
            assertThat(source.toArray(JAVA_BYTE)).isEqualTo(input);
            input[0] = 99;
            assertThat(source.get(JAVA_BYTE, 0)).isEqualTo((byte) 1);

            MemorySegment destination = buffers.output(MemorySegment.ofArray(output), output.length);
            destination.fill((byte) 7);
            NativeBuffers.copyOutput(destination, MemorySegment.ofArray(output), 3);
            assertThat(output).containsExactly(7, 7, 7, 42, 42, 42, 42, 42, 42, 42);
        }
    }

    @Test
    void testNativeBuffersKeepCallerOwnership()
    {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(16);
            try (NativeBuffers buffers = new NativeBuffers()) {
                assertThat(buffers.input(segment, segment.byteSize())).isEqualTo(segment);
                assertThat(buffers.output(segment, segment.byteSize())).isEqualTo(segment);
            }
            segment.fill((byte) 42);
            assertThat(segment.get(JAVA_BYTE, 0)).isEqualTo((byte) 42);
        }
    }

    @Test
    void testReadOnlyOutputRejected()
    {
        try (NativeBuffers buffers = new NativeBuffers()) {
            assertThatThrownBy(() -> buffers.output(MemorySegment.ofArray(new byte[10]).asReadOnly(), 10))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("read-only");
        }
    }
}
