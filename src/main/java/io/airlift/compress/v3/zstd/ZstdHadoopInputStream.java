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

import io.airlift.compress.v3.hadoop.HadoopInputStream;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

class ZstdHadoopInputStream
        extends HadoopInputStream
{
    private final InputStream in;
    private final boolean useNative;
    private ZstdInputStream zstdInputStream;
    private boolean closed;

    public ZstdHadoopInputStream(InputStream in, boolean useNative)
    {
        this.in = requireNonNull(in, "in is null");
        this.useNative = useNative;
    }

    @Override
    public int read()
            throws IOException
    {
        openStreamIfNecessary();
        return zstdInputStream.read();
    }

    @Override
    public int read(byte[] b)
            throws IOException
    {
        openStreamIfNecessary();
        return zstdInputStream.read(b);
    }

    @Override
    public int read(byte[] outputBuffer, int outputOffset, int outputLength)
            throws IOException
    {
        openStreamIfNecessary();
        return zstdInputStream.read(outputBuffer, outputOffset, outputLength);
    }

    @Override
    public void resetState()
    {
        if (closed) {
            return;
        }
        // Reset buffered decoder state after repositioning the source
        if (zstdInputStream instanceof ZstdNativeInputStream nativeInputStream) {
            // Reuse the native context; a dropped one would hold native memory until garbage collection
            nativeInputStream.resetState();
        }
        else {
            // recreate lazily on next read
            zstdInputStream = null;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            if (zstdInputStream != null) {
                zstdInputStream.close();
            }
            else {
                in.close();
            }
        }
    }

    // Lazy init avoids native stream allocations for streams that are never read,
    // and lets resetState() drop a Java stream so the next read starts a fresh one.
    private void openStreamIfNecessary()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (zstdInputStream == null) {
            zstdInputStream = useNative ? new ZstdNativeInputStream(in) : new ZstdJavaInputStream(in);
        }
    }
}
