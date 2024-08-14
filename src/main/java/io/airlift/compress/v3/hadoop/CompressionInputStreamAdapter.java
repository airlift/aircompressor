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
package io.airlift.compress.v3.hadoop;

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

final class CompressionInputStreamAdapter
        extends CompressionInputStream
{
    private static final InputStream FAKE_INPUT_STREAM = new InputStream()
    {
        @Override
        public int read()
        {
            throw new UnsupportedOperationException();
        }
    };

    private final HadoopInputStream input;
    private final PositionSupplier positionSupplier;

    public CompressionInputStreamAdapter(HadoopInputStream input, PositionSupplier positionSupplier)
            throws IOException
    {
        super(FAKE_INPUT_STREAM);
        this.input = requireNonNull(input, "input is null");
        this.positionSupplier = requireNonNull(positionSupplier, "positionSupplier is null");
    }

    @Override
    public int read()
            throws IOException
    {
        return input.read();
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        return input.read(b, off, len);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return positionSupplier.getPosition();
    }

    @Override
    public void resetState()
    {
        input.resetState();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            super.close();
        }
        finally {
            input.close();
        }
    }

    public interface PositionSupplier
    {
        long getPosition()
                throws IOException;
    }
}
