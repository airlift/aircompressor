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
package io.airlift.compress.snappy;

/**
 * Constants for implementing x-snappy-framed.
 */
final class SnappyFramed
{
    public static final int COMPRESSED_DATA_FLAG = 0x00;

    public static final int UNCOMPRESSED_DATA_FLAG = 0x01;

    public static final int STREAM_IDENTIFIER_FLAG = 0xff;

    /**
     * The header consists of the stream identifier flag, 3 bytes indicating a
     * length of 6, and "sNaPpY" in ASCII.
     */
    public static final byte[] HEADER_BYTES = new byte[] {(byte) STREAM_IDENTIFIER_FLAG, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59};

    private SnappyFramed()
    {
    }
}
