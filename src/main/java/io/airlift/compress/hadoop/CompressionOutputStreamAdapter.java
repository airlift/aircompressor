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
package io.airlift.compress.hadoop;

import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

final class CompressionOutputStreamAdapter
        extends CompressionOutputStream
{
    private static final OutputStream FAKE_OUTPUT_STREAM = new OutputStream() {
        @Override
        public void write(int b)
        {
            throw new UnsupportedOperationException();
        }
    };

    private final HadoopOutputStream output;

    public CompressionOutputStreamAdapter(HadoopOutputStream output)
    {
        super(FAKE_OUTPUT_STREAM);
        this.output = output;
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        output.write(b, off, len);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        output.write(b);
    }

    @Override
    public void finish()
            throws IOException
    {
        output.finish();
    }

    @Override
    public void resetState() {}

    @Override
    public void close()
            throws IOException
    {
        try {
            super.close();
        }
        finally {
            output.close();
        }
    }
}
