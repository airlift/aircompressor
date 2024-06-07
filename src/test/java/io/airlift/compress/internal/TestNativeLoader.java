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
package io.airlift.compress.internal;

import io.airlift.compress.internal.NativeLoader.Symbols;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static java.lang.invoke.MethodHandles.lookup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestNativeLoader
{
    @Test
    void testUnknownLibrary()
    {
        assertThatThrownBy(() -> NativeLoader.loadLibrary("unknown"))
                .isInstanceOf(LinkageError.class)
                .hasMessageMatching("Library not found: /aircompressor/.*/.*unknown.*");
    }

    @Test
    void testLoadSymbols()
            throws Throwable
    {
        Symbols<ValidMethodHandle> methodHandles = NativeLoader.loadSymbols("zstd", ValidMethodHandle.class, lookup());
        assertThat(methodHandles.linkageError()).isEmpty();
        assertThat(methodHandles.symbols().defaultCLevel().invoke()).isEqualTo(3);

        // loadSymbols requires a record class
        assertThatThrownBy(() -> NativeLoader.loadSymbols("zstd", Object.class, lookup()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("methodHandlesClass is not a record class");

        // loadSymbols requires a constructor visible to the lookup
        assertThatThrownBy(() -> NativeLoader.loadSymbols("zstd", ValidMethodHandle.class, MethodHandles.publicLookup()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Failed to find canonical constructor for %s".formatted(ValidMethodHandle.class));

        // unknown library
        Symbols<ValidMethodHandle> unknownLibrary = NativeLoader.loadSymbols("unknown", ValidMethodHandle.class, lookup());
        assertThat(unknownLibrary.linkageError()).isPresent();
        assertThatThrownBy(() -> unknownLibrary.symbols().defaultCLevel().invoke())
                .isInstanceOf(LinkageError.class)
                .hasMessageMatching("unknown native library not loaded: Library not found: /aircompressor/.*/.*unknown.*")
                .cause()
                .isInstanceOf(LinkageError.class)
                .hasMessageMatching("Library not found: /aircompressor/.*/.*unknown.*")
                .isEqualTo(unknownLibrary.linkageError().get());

        // missing annotation
        assertThatThrownBy(() -> NativeLoader.loadSymbols("zstd", MissingAnnotation.class, lookup()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("methodHandlesClass %s field 'missingAnnotation' is missing @NativeSignature annotation".formatted(MissingAnnotation.class));

        // constructor exception
        assertThatThrownBy(() -> NativeLoader.loadSymbols("zstd", ThrowsException.class, lookup()))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to create instance of %s".formatted(ThrowsException.class))
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("constructor exception");

        // unknown symbol
        Symbols<InvalidMethodHandle> invalidMethodHandles = NativeLoader.loadSymbols("zstd", InvalidMethodHandle.class, lookup());
        assertThat(invalidMethodHandles.linkageError()).isPresent();
        assertThatThrownBy(() -> invalidMethodHandles.symbols().unknown().invoke())
                .isInstanceOf(LinkageError.class)
                .hasMessage("zstd native library not loaded: unresolved symbol: unknownSymbol")
                .cause()
                .isInstanceOf(LinkageError.class)
                .hasMessage("unresolved symbol: unknownSymbol")
                .isEqualTo(invalidMethodHandles.linkageError().get());

        // all methods are throw when there is a single unresolved symbol
        assertThatThrownBy(() -> invalidMethodHandles.symbols().defaultCLevel().invoke())
                .isInstanceOf(LinkageError.class)
                .cause()
                .isInstanceOf(LinkageError.class)
                .hasMessage("unresolved symbol: unknownSymbol")
                .isEqualTo(invalidMethodHandles.linkageError().get());
    }

    private record ValidMethodHandle(
            @NativeSignature(name = "ZSTD_defaultCLevel", returnType = int.class, argumentTypes = {})
            MethodHandle defaultCLevel) {}

    private record InvalidMethodHandle(
            @NativeSignature(name = "ZSTD_defaultCLevel", returnType = int.class, argumentTypes = {})
            MethodHandle defaultCLevel,
            @NativeSignature(name = "unknownSymbol", returnType = int.class, argumentTypes = {})
            MethodHandle unknown) {}

    private record MissingAnnotation(
            @NativeSignature(name = "ZSTD_defaultCLevel", returnType = int.class, argumentTypes = {})
            MethodHandle defaultCLevel,
            MethodHandle missingAnnotation) {}

    private record ThrowsException(
            @NativeSignature(name = "ZSTD_defaultCLevel", returnType = int.class, argumentTypes = {})
            MethodHandle defaultCLevel)
    {
        private ThrowsException
        {
            throw new IllegalStateException("constructor exception");
        }
    }
}
