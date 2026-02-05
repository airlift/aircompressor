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

import java.io.InputStream;

/**
 * Abstract base class for zstd decompressing input streams.
 * <p>
 * This class provides a common type for both the pure Java implementation ({@link ZstdJavaInputStream})
 * and the native implementation ({@link ZstdNativeInputStream}).
 */
public abstract sealed class ZstdInputStream
        extends InputStream
        permits ZstdJavaInputStream, ZstdNativeInputStream {}
