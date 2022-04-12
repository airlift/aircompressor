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

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHashFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static com.google.common.io.ByteStreams.read;

public class JPountzLz4FrameDecompressor
        implements Decompressor
{
    private final LZ4SafeDecompressor decompressor;

    public JPountzLz4FrameDecompressor(LZ4Factory factory)
    {
        decompressor = factory.safeDecompressor();
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(input, inputOffset, inputLength);
                LZ4FrameInputStream decompressingInputStream = new LZ4FrameInputStream(inputStream, decompressor, XXHashFactory.fastestInstance().hash32())) {
            return read(decompressingInputStream, output, outputOffset, maxOutputLength);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
    {
        throw new UnsupportedOperationException();
    }
}
