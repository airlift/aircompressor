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

import io.airlift.compress.hadoop.HadoopInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

class JdkGzipHadoopInputStream
        extends HadoopInputStream
{
    private final byte[] oneByte = new byte[1];
    private final GZIPInputStream input;

    public JdkGzipHadoopInputStream(InputStream input, int bufferSize)
            throws IOException
    {
        this.input = new GZIPInputStream(new GzipBufferedInputStream(input, bufferSize), bufferSize);
    }

    @Override
    public int read()
            throws IOException
    {
        int length = input.read(oneByte, 0, 1);
        if (length < 0) {
            return length;
        }
        return oneByte[0] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        return input.read(output, offset, length);
    }

    @Override
    public void resetState()
    {
        throw new UnsupportedOperationException("resetState not supported for gzip");
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    // workaround for https://bugs.openjdk.org/browse/JDK-8081450
    private static class GzipBufferedInputStream
            extends BufferedInputStream
    {
        public GzipBufferedInputStream(InputStream input, int bufferSize)
        {
            super(requireNonNull(input, "input is null"), bufferSize);
        }

        @Override
        public int available()
                throws IOException
        {
            // GZIPInputStream thinks the stream is complete if this returns zero
            return max(1, super.available());
        }
    }
}
