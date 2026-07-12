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
package io.airlift.compress.v3.lz4;

import io.airlift.compress.v3.AbstractTestCompression;
import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;

class TestLz4FrameJava
        extends AbstractTestCompression
{
    @Override
    protected Compressor getCompressor()
    {
        return new Lz4FrameJavaCompressor();
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new Lz4FrameJavaDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new Lz4FrameJavaCompressor();
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new Lz4FrameJavaDecompressor();
    }
}
