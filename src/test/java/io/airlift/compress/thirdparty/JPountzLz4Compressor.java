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

import java.nio.ByteBuffer;

public class JPountzLz4Compressor
        implements Compressor
{
    private final LZ4Compressor compressor;

    public JPountzLz4Compressor(LZ4Factory factory)
    {
        compressor = factory.fastCompressor();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return compressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        return compressor.compress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        compressor.compress(input, output);
    }
}
