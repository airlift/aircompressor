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

import io.airlift.compress.v3.Compressor;

/**
 * Compresses data into the standard LZ4 frame format (as produced by the {@code lz4} command line tool),
 * as opposed to the raw LZ4 block format handled by {@link Lz4Compressor}.
 */
public interface Lz4FrameCompressor
        extends Compressor
{
    static Lz4FrameCompressor create()
    {
        if (Lz4FrameNativeCompressor.isEnabled()) {
            return new Lz4FrameNativeCompressor();
        }
        return new Lz4FrameJavaCompressor();
    }
}
