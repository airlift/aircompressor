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
package io.airlift.compress.v3.thirdparty;

import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.snappy.SnappyJavaCompressor;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

public class XerialSnappyCompressor
        implements Compressor
{
    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return new SnappyJavaCompressor().maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        try {
            return org.xerial.snappy.Snappy.compress(input, inputOffset, inputLength, output, outputOffset);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
