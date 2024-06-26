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

/**
 * Base class for zstd compressing output streams.
 * <p>
 * This class provides a common type for both the pure Java implementation ({@link ZstdJavaOutputStream})
 * and the native implementation ({@link ZstdNativeOutputStream}).
 * <p>
 * Only the native implementation emits the data written so far on {@link #flush()}, ending the current
 * block and flushing the underlying stream. The Java implementation's {@code flush()} writes nothing.
 */
public abstract sealed class ZstdOutputStream
        extends OutputStream
        permits ZstdJavaOutputStream, ZstdNativeOutputStream
{
    /**
     * Creates a compressing stream that uses the native zstd library when it is available,
     * and the pure Java implementation otherwise.
     */
    public static ZstdOutputStream create(OutputStream out)
            throws IOException
    {
        if (ZstdNativeOutputStream.isEnabled()) {
            return new ZstdNativeOutputStream(out);
        }
        return new ZstdJavaOutputStream(out);
    }

    /**
     * Creates a compressing stream at the given compression level. The pure Java implementation
     * supports only the default level, so any other level requires the native zstd library.
     *
     * @throws IllegalArgumentException if the native library is not available and the level is not the default
     */
    public static ZstdOutputStream create(OutputStream out, int compressionLevel)
            throws IOException
    {
        if (ZstdNativeOutputStream.isEnabled()) {
            return new ZstdNativeOutputStream(out, compressionLevel);
        }
        if (compressionLevel != CompressionParameters.DEFAULT_COMPRESSION_LEVEL) {
            throw new IllegalArgumentException("The Java Zstd output stream supports only the default compression level, not " + compressionLevel);
        }
        return new ZstdJavaOutputStream(out);
    }

    /**
     * Finishes the compression stream without closing the underlying output stream.
     * This is useful for Hadoop compatibility where the codec may need to finish
     * compression while keeping the underlying stream open. Writing after this call fails,
     * and a later {@link #close()} still closes the underlying stream.
     *
     * @throws IOException if an I/O error occurs
     */
    abstract void finishWithoutClosingSource()
            throws IOException;
}
