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
package io.airlift.compress.thirdparty;

import io.airlift.compress.Compressor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.xxhash.XXHashFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static java.lang.String.format;

public class JPountzLz4FrameCompressor
        implements Compressor
{
    private final LZ4Compressor compressor;

    public JPountzLz4FrameCompressor(LZ4Factory factory)
    {
        compressor = factory.fastCompressor();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        int maxHeaderLength;
        try {
            Field maxHeaderLengthField = LZ4FrameOutputStream.class.getDeclaredField("LZ4_MAX_HEADER_LENGTH");
            maxHeaderLengthField.setAccessible(true);
            maxHeaderLength = maxHeaderLengthField.getInt(null);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        return maxHeaderLength + compressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            try (LZ4FrameOutputStream compressingOutputStream = new LZ4FrameOutputStream(
                    outputStream,
                    LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB,
                    inputLength,
                    compressor,
                    XXHashFactory.fastestInstance().hash32(),
                    LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                    LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE)) {
                compressingOutputStream.write(input, inputOffset, inputLength);
            }
            byte[] compressed = outputStream.toByteArray();
            if (compressed.length > maxOutputLength) {
                throw new IllegalArgumentException(format("Output buffer too small, provided capacity %s, compressed data size %s", maxOutputLength, compressed.length));
            }
            System.arraycopy(compressed, 0, output, outputOffset, compressed.length);
            return compressed.length;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        throw new UnsupportedOperationException();
    }
}
