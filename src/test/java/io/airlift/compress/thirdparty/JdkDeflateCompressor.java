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

import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import static java.util.zip.Deflater.FULL_FLUSH;

public class JdkDeflateCompressor
        implements Compressor
{
    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return (int) ((uncompressedSize * 1.2) + 11);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        Deflater deflater = new Deflater(6, true);
        deflater.setInput(input, inputOffset, inputLength);
        deflater.finish();
        int compressedDataLength = deflater.deflate(output, outputOffset, maxOutputLength, FULL_FLUSH);
        deflater.end();
        return compressedDataLength;
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
