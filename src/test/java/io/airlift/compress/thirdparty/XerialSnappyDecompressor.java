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

import java.io.IOException;
import java.nio.ByteBuffer;

public class XerialSnappyDecompressor
    implements Decompressor
{
    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        try {
            return org.xerial.snappy.Snappy.uncompress(input, inputOffset, inputLength, output, outputOffset);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
