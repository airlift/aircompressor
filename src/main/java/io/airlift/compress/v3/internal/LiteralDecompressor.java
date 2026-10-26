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
package io.airlift.compress.v3.internal;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * Copies literal-only blocks after validating their complete encoding, without native staging.
 * Other encodings are left to the codec, indicated by a negative return value.
 */
public final class LiteralDecompressor
{
    private LiteralDecompressor() {}

    public static long tryLz4(MemorySegment input, long inputLength, MemorySegment output, long outputLength)
    {
        if (inputLength == 0) {
            return -1;
        }
        long position = 1;
        long length = (input.get(JAVA_BYTE, 0) & 0xFF) >>> 4;
        if (length == 15) {
            int value;
            do {
                if (position == inputLength) {
                    return -1;
                }
                value = input.get(JAVA_BYTE, position++) & 0xFF;
                length += value;
            }
            while (value == 255);
        }
        if (length != inputLength - position || length > outputLength) {
            return -1;
        }
        MemorySegment.copy(input, position, output, 0, length);
        return length;
    }

    public static long trySnappy(MemorySegment input, long inputLength, MemorySegment output, long outputLength)
    {
        long expectedLength = 0;
        long position = 0;
        int value;
        do {
            if (position == inputLength || position == 5) {
                return -1;
            }
            value = input.get(JAVA_BYTE, position) & 0xFF;
            expectedLength |= (long) (value & 0x7F) << (position * 7);
            position++;
        }
        while ((value & 0x80) != 0);
        if (expectedLength > 0xFFFF_FFFFL || expectedLength > outputLength) {
            return -1;
        }
        long start = position;
        long decoded = 0;
        // Validate all tags before changing output, including the declared size and trailing input.
        while (position < inputLength) {
            long tag = literal(input, position, inputLength);
            if (tag < 0) {
                return -1;
            }
            long length = tag >>> 3;
            position += (tag & 7) + length;
            decoded += length;
            if (decoded > expectedLength) {
                return -1;
            }
        }
        if (decoded != expectedLength) {
            return -1;
        }
        position = start;
        long outputPosition = 0;
        while (position < inputLength) {
            long tag = literal(input, position, inputLength);
            int headerSize = (int) (tag & 7);
            long length = tag >>> 3;
            MemorySegment.copy(input, position + headerSize, output, outputPosition, length);
            position += headerSize + length;
            outputPosition += length;
        }
        return decoded;
    }

    private static long literal(MemorySegment input, long position, long limit)
    {
        int tag = input.get(JAVA_BYTE, position) & 0xFF;
        if ((tag & 3) != 0) {
            return -1;
        }
        int prefix = tag >>> 2;
        int headerSize = 1;
        long length;
        if (prefix < 60) {
            length = prefix + 1;
        }
        else {
            int bytes = prefix - 59;
            if (limit - position <= bytes) {
                return -1;
            }
            long encoded = 0;
            for (int i = 0; i < bytes; i++) {
                encoded |= (input.get(JAVA_BYTE, position + i + 1) & 0xFFL) << (i * Byte.SIZE);
            }
            length = encoded + 1;
            headerSize += bytes;
        }
        if (length > limit - position - headerSize) {
            return -1;
        }
        return (length << 3) | headerSize;
    }
}
