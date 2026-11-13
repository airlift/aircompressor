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

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestLz4NativeCompressor
{
    @Test
    void testUnavailableNativeLibrary()
            throws Exception
    {
        // Isolate native initialization without changing process-wide properties or other tests' native state.
        try (URLClassLoader loader = new URLClassLoader(
                new URL[] {Lz4NativeCompressor.class.getProtectionDomain().getCodeSource().getLocation()},
                ClassLoader.getPlatformClassLoader())
        {
            @Override
            public URL getResource(String name)
            {
                return name.startsWith("aircompressor/") ? null : super.getResource(name);
            }
        }) {
            Class<?> compressor = loader.loadClass(Lz4NativeCompressor.class.getName());
            assertThat(compressor.getMethod("isEnabled").invoke(null)).isEqualTo(false);

            assertThatThrownBy(() -> compressor.getConstructor().newInstance())
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Lz4 native library is not enabled")
                    .cause().isInstanceOf(LinkageError.class);
            assertThatThrownBy(() -> compressor.getConstructor(int.class).newInstance(2))
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Lz4 native library is not enabled")
                    .cause().isInstanceOf(LinkageError.class);
        }
    }
}
