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

import io.airlift.compress.v3.Compressor;

import java.lang.WrongThreadException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static java.lang.Math.toIntExact;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * Prepares segments for Java codecs that access memory through Unsafe.
 */
public record MemorySegmentAccess(MemorySegment input, MemorySegment output, MemorySegment originalOutput)
{
    // Never started: accessibility from a different thread distinguishes confined segments.
    private static final Thread OTHER_THREAD = new Thread();

    public static MemorySegmentAccess of(MemorySegment input, MemorySegment output)
    {
        return of(input, output, output.byteSize());
    }

    public static MemorySegmentAccess forCompression(Compressor compressor, MemorySegment input, MemorySegment output)
    {
        long outputBound = output.byteSize();
        if (input.byteSize() <= Integer.MAX_VALUE) {
            int bound = compressor.maxCompressedLength((int) input.byteSize());
            // An overflowed int bound cannot be used to size staging memory.
            if (bound >= 0) {
                outputBound = Math.min(outputBound, bound);
            }
        }
        return of(input, output, outputBound);
    }

    private static MemorySegmentAccess of(MemorySegment input, MemorySegment output, long outputBound)
    {
        checkAccessible(input);
        checkWritable(output);

        // A different thread can close a shared arena while Unsafe is using its address. Checked
        // copies protect those accesses, including the final output copy if the arena was closed.
        // Confined arenas cannot be closed by another thread, and global memory cannot be closed.
        MemorySegment checkedInput = requiresCopy(input) || (!input.isNative() && input.isReadOnly())
                ? MemorySegment.ofArray(input.toArray(JAVA_BYTE))
                : input;
        MemorySegment checkedOutput = requiresCopy(output)
                ? MemorySegment.ofArray(new byte[toIntExact(outputBound)])
                : output;
        return new MemorySegmentAccess(checkedInput, checkedOutput, output);
    }

    public int finish(int written)
    {
        if (output != originalOutput) {
            MemorySegment.copy(output, 0, originalOutput, 0, written);
        }
        return written;
    }

    public static void checkWritable(MemorySegment output)
    {
        checkAccessible(output);
        if (output.isReadOnly()) {
            throw new IllegalArgumentException("Output MemorySegment is read-only");
        }
    }

    private static void checkAccessible(MemorySegment segment)
    {
        if (!segment.isAccessibleBy(Thread.currentThread())) {
            throw new WrongThreadException("MemorySegment is not accessible by the current thread");
        }
        if (!segment.scope().isAlive()) {
            throw new IllegalStateException("MemorySegment is closed");
        }
    }

    private static boolean requiresCopy(MemorySegment segment)
    {
        return segment.isNative() &&
                !segment.scope().equals(Arena.global().scope()) &&
                segment.isAccessibleBy(OTHER_THREAD);
    }
}
