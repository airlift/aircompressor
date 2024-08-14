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
package io.airlift.compress.v3.bzip2;

import io.airlift.compress.v3.hadoop.HadoopOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

// forked from Apache Hadoop
class BZip2HadoopOutputStream
        extends HadoopOutputStream
{
    private final OutputStream rawOutput;
    private boolean initialized;
    private CBZip2OutputStream output;

    public BZip2HadoopOutputStream(OutputStream out)
    {
        this.rawOutput = requireNonNull(out, "out is null");
    }

    @Override
    public void write(int b)
            throws IOException
    {
        openStreamIfNecessary();
        output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        openStreamIfNecessary();
        output.write(b, off, len);
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

    @Override
    public void flush()
            throws IOException
    {
        rawOutput.flush();
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
            rawOutput.close();
        }
    }

    private void openStreamIfNecessary()
            throws IOException
    {
        if (output == null) {
            initialized = true;
            // write magic
            rawOutput.write(new byte[] {'B', 'Z'});
            // open new block
            output = new CBZip2OutputStream(rawOutput);
        }
    }
}
