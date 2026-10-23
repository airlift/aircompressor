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

import io.airlift.compress.v3.hadoop.HadoopOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

class ZstdHadoopOutputStream
        extends HadoopOutputStream
{
    private final OutputStream out;
    private final boolean useNative;
    private boolean initialized;
    private ZstdOutputStreamBase zstdOutputStream;

    public ZstdHadoopOutputStream(OutputStream out, boolean useNative)
    {
        this.out = requireNonNull(out, "out is null");
        this.useNative = useNative;
    }

    @Override
    public void write(int b)
            throws IOException
    {
        openStreamIfNecessary();
        zstdOutputStream.write(b);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        openStreamIfNecessary();
        zstdOutputStream.write(buffer, offset, length);
    }

    @Override
    public void finish()
            throws IOException
    {
        if (zstdOutputStream != null) {
            zstdOutputStream.finishWithoutClosingSource();
            zstdOutputStream = null;
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        // the native stream flushes the destination after the compressor; the Java stream buffers until finished
        // and flushes nothing, as does a stream that was never written to or was finished
        if (zstdOutputStream instanceof ZstdNativeOutputStream) {
            zstdOutputStream.flush();
        }
        else {
            out.flush();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            // If the stream has never been initialized, create a valid empty file
            if (!initialized) {
                openStreamIfNecessary();
            }
            finish();
        }
        finally {
            out.close();
        }
    }

    private void openStreamIfNecessary()
            throws IOException
    {
        if (zstdOutputStream == null) {
            initialized = true;
            zstdOutputStream = useNative ? new ZstdNativeOutputStream(out) : new ZstdJavaOutputStream(out);
        }
    }
}
