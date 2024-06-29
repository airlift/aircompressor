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
package io.airlift.compress.v2.snappy;

import java.io.IOException;
import java.io.InputStream;

final class SnappyInternalUtils
{
    private SnappyInternalUtils()
    {
    }

    //
    // Copied from Guava Preconditions
    static <T> T checkNotNull(T reference, String errorMessageTemplate, Object... errorMessageArgs)
    {
        if (reference == null) {
            // If either of these parameters is null, the right thing happens anyway
            throw new NullPointerException(String.format(errorMessageTemplate, errorMessageArgs));
        }
        return reference;
    }

    static void checkArgument(boolean expression, String errorMessageTemplate, Object... errorMessageArgs)
    {
        if (!expression) {
            throw new IllegalArgumentException(String.format(errorMessageTemplate, errorMessageArgs));
        }
    }

    static void checkPositionIndexes(int start, int end, int size)
    {
        // Carefully optimized for execution by hotspot (explanatory comment above)
        if (start < 0 || end < start || end > size) {
            throw new IndexOutOfBoundsException(badPositionIndexes(start, end, size));
        }
    }

    static String badPositionIndexes(int start, int end, int size)
    {
        if (start < 0 || start > size) {
            return badPositionIndex(start, size, "start index");
        }
        if (end < 0 || end > size) {
            return badPositionIndex(end, size, "end index");
        }
        // end < start
        return String.format("end index (%s) must not be less than start index (%s)", end, start);
    }

    static String badPositionIndex(int index, int size, String desc)
    {
        if (index < 0) {
            return String.format("%s (%s) must not be negative", desc, index);
        }
        else if (size < 0) {
            throw new IllegalArgumentException("negative size: " + size);
        }
        else { // index > size
            return String.format("%s (%s) must not be greater than size (%s)", desc, index, size);
        }
    }

    /**
     * Reads <i>length</i> bytes from <i>source</i> into <i>dest</i> starting at <i>offset</i>. <br/>
     * <p/>
     * The only case where the <i>length</i> <tt>byte</tt>s will not be read is if <i>source</i> returns EOF.
     *
     * @param source The source of bytes to read from. Must not be <code>null</code>.
     * @param dest The <tt>byte[]</tt> to read bytes into. Must not be <code>null</code>.
     * @param offset The index in <i>dest</i> to start filling.
     * @param length The number of bytes to read.
     * @return Total number of bytes actually read.
     * @throws IndexOutOfBoundsException if <i>offset</i> or <i>length</i> are invalid.
     */
    static int readBytes(InputStream source, byte[] dest, int offset, int length)
            throws IOException
    {
        checkNotNull(source, "source is null");
        checkNotNull(dest, "dest is null");

        // how many bytes were read.
        int lastRead = source.read(dest, offset, length);

        int totalRead = lastRead;

        // if we did not read as many bytes as we had hoped, try reading again.
        if (lastRead < length) {
            // as long the buffer is not full (remaining() == 0) and we have not reached EOF (lastRead == -1) keep reading.
            while (totalRead < length && lastRead != -1) {
                lastRead = source.read(dest, offset + totalRead, length - totalRead);

                // if we got EOF, do not add to total read.
                if (lastRead != -1) {
                    totalRead += lastRead;
                }
            }
        }

        return totalRead;
    }

    static int skip(InputStream source, int skip)
            throws IOException
    {
        // optimization also avoids potential for error with some implementation of
        // InputStream.skip() which throw exceptions with negative numbers (ie. ZipInputStream).
        if (skip <= 0) {
            return 0;
        }

        int toSkip = skip - (int) source.skip(skip);

        boolean more = true;
        while (toSkip > 0 && more) {
            // check to see if we reached EOF
            int read = source.read();
            if (read == -1) {
                more = false;
            }
            else {
                --toSkip;
                toSkip -= source.skip(toSkip);
            }
        }

        int skipped = skip - toSkip;

        return skipped;
    }
}
