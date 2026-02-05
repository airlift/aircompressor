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
 * Base class for zstd compressing output streams.
 * <p>
 * This class provides a common type for both the pure Java implementation ({@link ZstdJavaOutputStream})
 * and the native implementation ({@link ZstdNativeOutputStream}).
 */
public abstract sealed class ZstdOutputStream
        extends OutputStream
        permits ZstdJavaOutputStream, ZstdNativeOutputStream
{
    /**
     * Finishes the compression stream without closing the underlying output stream.
     * This is useful for Hadoop compatibility where the codec may need to finish
     * compression while keeping the underlying stream open.
     *
     * @throws IOException if an I/O error occurs
     */
    public abstract void finishWithoutClosingSource()
            throws IOException;
}
