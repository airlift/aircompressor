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
import java.nio.charset.StandardCharsets;

import static io.airlift.compress.bzip2.BZip2Constants.HEADER;

// forked from Apache Hadoop
class BZip2CompressionOutputStream
        extends CompressionOutputStream
{
    private CBZip2OutputStream output;
    private boolean needsReset;

    public BZip2CompressionOutputStream(OutputStream out)
    {
        super(out);
        needsReset = true;
    }

    private void writeStreamHeader()
            throws IOException
    {
        if (out != null) {
            // The compressed bzip2 stream should start with the
            // identifying characters BZ. Caller of CBZip2OutputStream
            // i.e. this class must write these characters.
            out.write(HEADER.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        if (needsReset) {
            // In the case that nothing is written to this stream, we still need to
            // write out the header before closing, otherwise the stream won't be
            // recognized by BZip2CompressionInputStream.
            internalReset();
        }
        this.output.finish();
        needsReset = true;
    }

    private void internalReset()
            throws IOException
    {
        if (needsReset) {
            needsReset = false;
            writeStreamHeader();
            this.output = new CBZip2OutputStream(out);
        }
    }

    @Override
    public void resetState()
    {
        // Cannot write to out at this point because out might not be ready
        // yet, as in SequenceFile.Writer implementation.
        needsReset = true;
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (needsReset) {
            internalReset();
        }
        this.output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        if (needsReset) {
            internalReset();
        }
        this.output.write(b, off, len);
    }

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
