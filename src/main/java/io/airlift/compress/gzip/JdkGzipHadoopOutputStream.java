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
package io.airlift.compress.gzip;

import io.airlift.compress.hadoop.HadoopOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import static java.util.Objects.requireNonNull;

class JdkGzipHadoopOutputStream
        extends HadoopOutputStream
{
    private final byte[] oneByte = new byte[1];
    private final GZIPOutputStreamWrapper output;

    public JdkGzipHadoopOutputStream(OutputStream output, int bufferSize)
            throws IOException
    {
        this.output = new GZIPOutputStreamWrapper(requireNonNull(output, "output is null"), bufferSize);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        oneByte[0] = (byte) b;
        write(oneByte, 0, 1);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        output.write(buffer, offset, length);
    }

    @Override
    public void finish()
            throws IOException
    {
        try {
            output.finish();
        }
        finally {
            output.end();
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        output.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            finish();
        }
        finally {
            output.close();
        }
    }

    private static class GZIPOutputStreamWrapper
            extends GZIPOutputStream
    {
        GZIPOutputStreamWrapper(OutputStream output, int bufferSize)
                throws IOException
        {
            super(output, bufferSize);
        }

        public void end() throws IOException
        {
            // free the memory as early as possible
            def.end();
        }
    }
}
