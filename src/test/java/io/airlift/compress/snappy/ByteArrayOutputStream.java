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
package io.airlift.compress.snappy;

import com.google.common.base.Preconditions;

import java.io.OutputStream;

final public class ByteArrayOutputStream
        extends OutputStream
{
    private final byte buffer[];
    private final int initialOffset;
    private final int bufferLimit;
    private int offset;

    public ByteArrayOutputStream(byte[] buffer)
    {
        this(buffer, 0, buffer.length);
    }

    public ByteArrayOutputStream(byte[] buffer, int offset, int length)
    {
        this.buffer = buffer;
        this.initialOffset = offset;
        this.bufferLimit = offset + length;
        this.offset = offset;
    }

    @Override
    public void write(int b)
    {
        Preconditions.checkPositionIndex(offset + 1, bufferLimit);
        buffer[offset++] = (byte) b;
    }

    @Override
    public void write(byte b[], int off, int len)
    {
        Preconditions.checkPositionIndex(offset + len, bufferLimit);
        System.arraycopy(b, off, buffer, offset, len);
        offset += len;
    }

    public int size()
    {
        return offset - initialOffset;
    }

    public byte[] getBuffer()
    {
        return buffer;
    }
}
