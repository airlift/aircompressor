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
package io.airlift.compress.v3.zstd;

import java.io.IOException;
import java.io.InputStream;

import static io.airlift.compress.v3.zstd.Util.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Decompresses a Zstd stream. This class can be extended, and every read goes through
 * {@link #read(byte[], int, int)}.
 */
public class ZstdInputStream
        extends InputStream
{
    private final ZstdInputStreamBase delegate;
    private byte[] singleByteOutputBuffer;

    /**
     * Creates a decompressing stream that uses the pure Java implementation.
     * Use {@link #create(InputStream)} to use the native zstd library when it is available.
     */
    public ZstdInputStream(InputStream inputStream)
    {
        this(new ZstdJavaInputStream(inputStream));
    }

    ZstdInputStream(ZstdInputStreamBase delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    /**
     * Creates a decompressing stream that uses the native zstd library when it is available,
     * and the pure Java implementation otherwise.
     */
    public static ZstdInputStream create(InputStream in)
    {
        if (ZstdNativeInputStream.isEnabled()) {
            return new ZstdInputStream(new ZstdNativeInputStream(in));
        }
        return new ZstdInputStream(new ZstdJavaInputStream(in));
    }

    @Override
    public int read()
            throws IOException
    {
        if (singleByteOutputBuffer == null) {
            singleByteOutputBuffer = new byte[1];
        }
        int readSize = read(singleByteOutputBuffer, 0, 1);
        checkState(readSize != 0, "A zero read size should never be returned");
        if (readSize != 1) {
            return -1;
        }
        return singleByteOutputBuffer[0] & 0xFF;
    }

    @Override
    public int read(byte[] outputBuffer, int outputOffset, int outputLength)
            throws IOException
    {
        return delegate.read(outputBuffer, outputOffset, outputLength);
    }

    @Override
    public int available()
            throws IOException
    {
        return delegate.available();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    ZstdInputStreamBase delegate()
    {
        return delegate;
    }
}
