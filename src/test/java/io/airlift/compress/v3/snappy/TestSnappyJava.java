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
package io.airlift.compress.v3.snappy;

import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.thirdparty.XerialSnappyCompressor;
import io.airlift.compress.v3.thirdparty.XerialSnappyDecompressor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSnappyJava
        extends AbstractTestSnappy
{
    @Override
    protected SnappyCompressor getCompressor()
    {
        return new SnappyJavaCompressor();
    }

    @Override
    protected SnappyDecompressor getDecompressor()
    {
        return new SnappyJavaDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new XerialSnappyCompressor();
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new XerialSnappyDecompressor();
    }

    @Test
    void testZeroMatchOffsetFails()
    {
        byte[] zeroMatchOffset = new byte[] {16, 1, 0, 1, 0, 1, 0, 1, 0};
        assertThatThrownBy(() -> new SnappyJavaDecompressor().decompress(zeroMatchOffset, 0, zeroMatchOffset.length, new byte[64], 0, 64))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("Malformed input: offset=2");
    }
}
