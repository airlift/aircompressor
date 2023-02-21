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
package io.airlift.compress.bzip2;

import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

// forked from Apache Hadoop
class BZip2CompressionOutputStream
        extends CompressionOutputStream
{
    private boolean initialized;
    private CBZip2OutputStream output;

    public BZip2CompressionOutputStream(OutputStream out)
    {
        super(requireNonNull(out, "out is null"));
    }

    @Override
    public void finish()
            throws IOException
    {
        if (output != null) {
            output.finish();
            output = null;
        }
    }

    private void openStreamIfNecessary()
            throws IOException
    {
        if (output == null) {
            initialized = true;
            // write magic
            out.write(new byte[] {'B', 'Z'});
            // open new block
            this.output = new CBZip2OutputStream(out);
        }
    }

    @Override
    public void resetState() {}

    @Override
    public void write(int b)
            throws IOException
    {
        openStreamIfNecessary();
        this.output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        openStreamIfNecessary();
        this.output.write(b, off, len);
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            // it the stream has never been initialized, create an empty block
            if (!initialized) {
                openStreamIfNecessary();
            }
            finish();
        }
        finally {
            super.close();
        }
    }
}
