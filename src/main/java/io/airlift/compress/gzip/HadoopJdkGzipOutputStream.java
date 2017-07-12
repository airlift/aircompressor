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

import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

class HadoopJdkGzipOutputStream
        extends CompressionOutputStream
{
    private final byte[] oneByte = new byte[1];
    private final GZIPOutputStreamWrapper output;

    public HadoopJdkGzipOutputStream(OutputStream output, int bufferSize)
            throws IOException
    {
        super(output);
        this.output = new GZIPOutputStreamWrapper(output, bufferSize);
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
    public void resetState()
            throws IOException
    {
        output.finish();
    }

    private class GZIPOutputStreamWrapper
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
