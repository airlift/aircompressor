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

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

class HadoopJdkGzipInputStream
        extends CompressionInputStream
{
    private final byte[] oneByte = new byte[1];
    private final GZIPInputStream input;

    public HadoopJdkGzipInputStream(InputStream input, int bufferSize)
            throws IOException
    {
        super(input);
        this.input = new GZIPInputStream(input, bufferSize);
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
            throws IOException
    {
        throw new UnsupportedOperationException("resetState not supported for gzip");
    }

    @Override
    public void close() throws IOException
    {
        try {
            super.close();
        }
        finally {
            // close() will free the memory
            input.close();
        }
    }
}
