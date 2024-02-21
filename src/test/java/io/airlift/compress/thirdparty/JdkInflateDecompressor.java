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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static com.google.common.base.Preconditions.checkPositionIndexes;

public class JdkInflateDecompressor
        implements Decompressor
{
    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        checkPositionIndexes(inputOffset, inputOffset + inputLength, input.length);
        checkPositionIndexes(outputOffset, outputOffset + maxOutputLength, output.length);

        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(input, inputOffset, inputLength);
            int resultLength = inflater.inflate(output, outputOffset, maxOutputLength);
            if (!inflater.finished()) {
                throw new MalformedInputException(inflater.getBytesRead());
            }
            return resultLength;
        }
        catch (DataFormatException e) {
            throw new RuntimeException(e);
        }
        finally {
            inflater.end();
        }
    }

    @SuppressWarnings("RedundantCast") // allow running on JDK 8
    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
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

        int written = decompress(inputArray, inputOffset, inputLength, outputArray, outputOffset, outputLength);

        if (output.hasArray()) {
            ((Buffer) output).position(output.position() + written);
        }
        else {
            output.put(outputArray, outputOffset, written);
        }
    }
}
