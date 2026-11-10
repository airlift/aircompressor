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
package io.airlift.compress.v3.hadoop;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static java.lang.Math.max;
import static java.lang.Math.min;

@SuppressWarnings("AbstractMethodOverridesConcreteMethod")
public abstract class HadoopInputStream
        extends InputStream
{
    protected static byte[] readCompressedBlock(InputStream input, byte[] buffer, int length)
            throws IOException
    {
        if (length <= 0 || length > Integer.MAX_VALUE - Long.BYTES) {
            throw new IOException("Invalid compressed chunk length: " + length);
        }

        int offset = 0;
        while (offset < length) {
            // Grow only as data arrives, so a truncated chunk cannot allocate its declared size.
            // Keep the extra long of padding used by the raw decompressors.
            if (buffer.length - Long.BYTES <= offset) {
                int capacity = (int) min(length, max(8192L, (long) offset * 2));
                buffer = Arrays.copyOf(buffer, capacity + Long.BYTES);
            }
            int size = input.read(buffer, offset, min(length - offset, buffer.length - Long.BYTES - offset));
            if (size == -1) {
                throw new EOFException("encountered EOF while reading block data");
            }
            if (size == 0) {
                int value = input.read();
                if (value == -1) {
                    throw new EOFException("encountered EOF while reading block data");
                }
                buffer[offset++] = (byte) value;
            }
            else {
                offset += size;
            }
        }
        return buffer;
    }

    public abstract void resetState();

    @Override
    public abstract int read(byte[] b, int off, int len)
            throws IOException;

    @Override
    public abstract void close()
            throws IOException;
}
