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
package io.airlift.compress.v3.zstd;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The implementations of a Zstd output stream, which {@link ZstdOutputStream} wraps.
 */
abstract sealed class ZstdOutputStreamBase
        extends OutputStream
        permits ZstdJavaOutputStream, ZstdNativeOutputStream
{
    /**
     * Finishes the compression stream without closing the underlying output stream.
     * This is useful for Hadoop compatibility where the codec may need to finish
     * compression while keeping the underlying stream open. Writing after this call fails,
     * and a later {@link #close()} still closes the underlying stream. If finishing failed,
     * a later call to this method or to {@link #close()} fails too, as the frame was not ended.
     *
     * @throws IOException if an I/O error occurs
     */
    abstract void finishWithoutClosingSource()
            throws IOException;
}
