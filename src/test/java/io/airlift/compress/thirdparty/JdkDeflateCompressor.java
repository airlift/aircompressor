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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.util.zip.Deflater.FULL_FLUSH;

public class JdkDeflateCompressor
        implements Compressor
{
    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        // From Mark Adler's post http://stackoverflow.com/questions/1207877/java-size-of-compression-output-bytearray
        return uncompressedSize + ((uncompressedSize + 7) >> 3) + ((uncompressedSize + 63) >> 6) + 5;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        checkPositionIndexes(inputOffset, inputOffset + inputLength, input.length);
        checkPositionIndexes(outputOffset, outputOffset + maxOutputLength, output.length);

        if (maxOutputLength < maxCompressedLength(inputLength)) {
            throw new IllegalArgumentException("Max output length must be larger than " + maxCompressedLength(inputLength));
        }

        Deflater deflater = new Deflater(6, true);
        try {
            deflater.setInput(input, inputOffset, inputLength);
            deflater.finish();
            int size = deflater.deflate(output, outputOffset, maxOutputLength, FULL_FLUSH);
            if (!deflater.finished()) {
                throw new RuntimeException("maxCompressedLength formula is incorrect, because deflate produced more data");
            }
            return size;
        }
        finally {
            deflater.end();
        }
    }

    @SuppressWarnings("RedundantCast") // allow running on JDK 8
    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        byte[] inputArray;
        int inputOffset;
        int inputLength;
        if (input.hasArray()) {
            inputArray = input.array();
            inputOffset = input.arrayOffset() + input.position();
            inputLength = input.remaining();
        }
        else {
            inputArray = new byte[input.remaining()];
            inputOffset = 0;
            inputLength = inputArray.length;
            input.get(inputArray);
        }

        byte[] outputArray;
        int outputOffset;
        int outputLength;
        if (output.hasArray()) {
            outputArray = output.array();
            outputOffset = output.arrayOffset() + output.position();
            outputLength = output.remaining();
        }
        else {
            outputArray = new byte[output.remaining()];
            outputOffset = 0;
            outputLength = outputArray.length;
        }

        int written = compress(inputArray, inputOffset, inputLength, outputArray, outputOffset, outputLength);

        if (output.hasArray()) {
            ((Buffer) output).position(output.position() + written);
        }
        else {
            output.put(outputArray, outputOffset, written);
        }
    }
}
