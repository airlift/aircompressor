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
package io.airlift.compress.v3.deflate;

import io.airlift.compress.v3.hadoop.HadoopOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;

import static java.util.Objects.requireNonNull;

class JdkDeflateHadoopOutputStream
        extends HadoopOutputStream
{
    private final byte[] oneByte = new byte[1];
    private final OutputStream output;
    private final Deflater deflater;
    private final byte[] outputBuffer;
    protected boolean closed;

    public JdkDeflateHadoopOutputStream(OutputStream output, int bufferSize)
    {
        this.output = requireNonNull(output, "output is null");
        this.deflater = new Deflater();
        this.outputBuffer = new byte[bufferSize];
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
        deflater.setInput(buffer, offset, length);
        while (!deflater.needsInput()) {
            compress();
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        if (!deflater.finished()) {
            deflater.finish();
            while (!deflater.finished()) {
                compress();
            }
        }
        deflater.reset();
    }

    private void compress()
            throws IOException
    {
        int compressedSize = deflater.deflate(outputBuffer, 0, outputBuffer.length);
        output.write(outputBuffer, 0, compressedSize);
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
        if (!closed) {
            closed = true;
            try {
                finish();
            }
            finally {
                output.close();
            }
        }
    }
}
