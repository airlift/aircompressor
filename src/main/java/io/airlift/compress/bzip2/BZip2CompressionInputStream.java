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

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import static io.airlift.compress.bzip2.BZip2Constants.HEADER;
import static java.util.Objects.requireNonNull;

// forked from Apache Hadoop
class BZip2CompressionInputStream
        extends CompressionInputStream
{
    private static final int HEADER_LEN = HEADER.length();

    private CBZip2InputStream input;
    private final BufferedInputStream bufferedIn;

    public BZip2CompressionInputStream(InputStream in)
            throws IOException
    {
        super(requireNonNull(in, "in is null"));
        bufferedIn = new BufferedInputStream(in);
    }

    private void trySkipMagic()
            throws IOException
    {
        // If the stream starts with `BZ`, skip it
        bufferedIn.mark(HEADER_LEN);
        if (bufferedIn.read() != 'B' || bufferedIn.read() != 'Z') {
            bufferedIn.reset();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        input = null;
        super.close();
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        if (len == 0) {
            return 0;
        }

        if (input == null) {
            trySkipMagic();
            input = new CBZip2InputStream(bufferedIn);
        }

        int result;
        result = this.input.read(b, off, len);

        // if the result is the end of block marker, no data was read
        if (result == CBZip2InputStream.END_OF_BLOCK) {
            // read one byte into the new block and update the position.
            result = this.input.read(b, off, 1);
        }

        return result;
    }

    @Override
    public int read()
            throws IOException
    {
        byte[] b = new byte[1];
        int result = this.read(b, 0, 1);
        return (result < 0) ? result : (b[0] & 0xff);
    }

    @Override
    public void resetState()
    {
        // drop the current compression stream, and new one will be created during the next read
        input = null;
    }
}
