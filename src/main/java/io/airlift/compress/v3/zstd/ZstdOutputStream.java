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
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

/**
 * Compresses to a Zstd stream. This class can be extended, and every write goes through
 * {@link #write(byte[], int, int)}.
 */
public class ZstdOutputStream
        extends OutputStream
{
    private final ZstdOutputStreamBase delegate;
    private final byte[] singleByteBuffer = new byte[1];

    /**
     * Creates a compressing stream that uses the pure Java implementation.
     * Use {@link #create(OutputStream)} to use the native zstd library when it is available.
     */
    public ZstdOutputStream(OutputStream outputStream)
            throws IOException
    {
        this(new ZstdJavaOutputStream(outputStream));
    }

    ZstdOutputStream(ZstdOutputStreamBase delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    /**
     * Creates a compressing stream that uses the native zstd library when it is available,
     * and the pure Java implementation otherwise.
     */
    public static ZstdOutputStream create(OutputStream out)
            throws IOException
    {
        if (ZstdNativeOutputStream.isEnabled()) {
            return new ZstdOutputStream(new ZstdNativeOutputStream(out));
        }
        return new ZstdOutputStream(new ZstdJavaOutputStream(out));
    }

    /**
     * Creates a compressing stream at the given compression level, where 0 selects the default level.
     * The pure Java implementation supports only the default level, so any other level requires the native zstd library.
     *
     * @throws IllegalArgumentException if the native library is not available and the level is not the default
     */
    public static ZstdOutputStream create(OutputStream out, int compressionLevel)
            throws IOException
    {
        if (ZstdNativeOutputStream.isEnabled()) {
            return new ZstdOutputStream(new ZstdNativeOutputStream(out, compressionLevel));
        }
        // libzstd treats level 0 as the default level, so the same call must not depend on whether the native library loads
        if (compressionLevel != 0 && compressionLevel != CompressionParameters.DEFAULT_COMPRESSION_LEVEL) {
            throw new IllegalArgumentException("The Java Zstd output stream supports only the default compression level, not " + compressionLevel);
        }
        return new ZstdOutputStream(new ZstdJavaOutputStream(out));
    }

    @Override
    public void write(int b)
            throws IOException
    {
        singleByteBuffer[0] = (byte) b;
        write(singleByteBuffer, 0, 1);
    }

    @Override
    public void write(byte[] buffer)
            throws IOException
    {
        write(buffer, 0, buffer.length);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        delegate.write(buffer, offset, length);
    }

    @Override
    public void flush()
            throws IOException
    {
        delegate.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    /**
     * Finishes the compression stream without closing the underlying output stream.
     * Writing after this call fails, and a later {@link #close()} still closes the underlying stream.
     * If finishing failed, a later {@link #close()} fails too, as the frame was not ended.
     */
    void finishWithoutClosingSource()
            throws IOException
    {
        delegate.finishWithoutClosingSource();
    }

    ZstdOutputStreamBase delegate()
    {
        return delegate;
    }
}
