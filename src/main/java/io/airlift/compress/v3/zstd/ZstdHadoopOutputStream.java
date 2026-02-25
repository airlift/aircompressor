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
    private ZstdOutputStream zstdOutputStream;

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
        out.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            // If the stream has never been initialized, create a valid empty file
            openStreamIfNecessary();
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
            if (useNative) {
                zstdOutputStream = new ZstdNativeOutputStream(out);
            }
            else {
                zstdOutputStream = new ZstdJavaOutputStream(out);
            }
        }
    }
}
