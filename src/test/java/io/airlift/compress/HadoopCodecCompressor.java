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
package io.airlift.compress;

import io.airlift.compress.snappy.ByteArrayOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.function.IntUnaryOperator;

import static java.util.Objects.requireNonNull;

public class HadoopCodecCompressor
        implements Compressor
{
    private final CompressionCodec codec;
    private final IntUnaryOperator blockCompressorMaxCompressedLength;

    public HadoopCodecCompressor(CompressionCodec codec, Compressor blockCompressorForSizeCalculation)
    {
        this(codec, blockCompressorForSizeCalculation::maxCompressedLength);
    }

    public HadoopCodecCompressor(CompressionCodec codec, IntUnaryOperator blockCompressorMaxCompressedLength)
    {
        this.codec = requireNonNull(codec, "codec is null");
        this.blockCompressorMaxCompressedLength = requireNonNull(blockCompressorMaxCompressedLength, "blockCompressorMaxCompressedLength is null");
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        // assume hadoop stream encoder won't increase size by more than 10% over the block encoder
        return (int) ((blockCompressorMaxCompressedLength.applyAsInt(uncompressedSize) * 1.1) + 8);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(output, outputOffset, maxOutputLength);

        try {
            OutputStream out = codec.createOutputStream(byteArrayOutputStream);
            // write in a single shot to cause multiple chunks per block
            out.write(input, inputOffset, inputLength);
            out.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return byteArrayOutputStream.size();
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
