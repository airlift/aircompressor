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
package io.airlift.compress.v3.xxhash;

import io.airlift.compress.v3.internal.NativeLoader;
import io.airlift.compress.v3.internal.NativeLoader.Symbols;
import io.airlift.compress.v3.internal.NativeSignature;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static java.lang.invoke.MethodHandles.lookup;

final class XxHash64Bindings
{
    private XxHash64Bindings() {}

    private record MethodHandles(
            // One-shot hash
            @NativeSignature(name = "XXH64", returnType = long.class, argumentTypes = {MemorySegment.class, long.class, long.class})
            MethodHandle hash,
            // State management
            @NativeSignature(name = "XXH64_createState", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle createState,
            @NativeSignature(name = "XXH64_freeState", returnType = int.class, argumentTypes = MemorySegment.class)
            MethodHandle freeState,
            // Streaming
            @NativeSignature(name = "XXH64_reset", returnType = int.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle reset,
            @NativeSignature(name = "XXH64_update", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class})
            MethodHandle update,
            @NativeSignature(name = "XXH64_digest", returnType = long.class, argumentTypes = MemorySegment.class)
            MethodHandle digest) {}

    private static final Optional<LinkageError> LINKAGE_ERROR;

    private static final MethodHandle HASH_METHOD;
    private static final MethodHandle CREATE_STATE_METHOD;
    private static final MethodHandle FREE_STATE_METHOD;
    private static final MethodHandle RESET_METHOD;
    private static final MethodHandle UPDATE_METHOD;
    private static final MethodHandle DIGEST_METHOD;

    static {
        Symbols<MethodHandles> symbols = NativeLoader.loadSymbols("xxhash", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        MethodHandles methodHandles = symbols.symbols();

        HASH_METHOD = methodHandles.hash();
        CREATE_STATE_METHOD = methodHandles.createState();
        FREE_STATE_METHOD = methodHandles.freeState();
        RESET_METHOD = methodHandles.reset();
        UPDATE_METHOD = methodHandles.update();
        DIGEST_METHOD = methodHandles.digest();
    }

    public static boolean isEnabled()
    {
        return LINKAGE_ERROR.isEmpty();
    }

    public static void verifyEnabled()
    {
        if (LINKAGE_ERROR.isPresent()) {
            throw new IllegalStateException("XxHash64 native library is not enabled", LINKAGE_ERROR.get());
        }
    }

    public static long hash(MemorySegment input, long length, long seed)
    {
        try {
            return (long) HASH_METHOD.invokeExact(input, length, seed);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static MemorySegment createState()
    {
        try {
            return (MemorySegment) CREATE_STATE_METHOD.invokeExact();
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static void freeState(MemorySegment state)
    {
        try {
            int result = (int) FREE_STATE_METHOD.invokeExact(state);
            if (result != 0) {
                throw new IllegalStateException("XXH64_freeState failed: " + result);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static void reset(MemorySegment state, long seed)
    {
        try {
            int result = (int) RESET_METHOD.invokeExact(state, seed);
            if (result != 0) {
                throw new IllegalStateException("XXH64_reset failed: " + result);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static void update(MemorySegment state, MemorySegment input, long length)
    {
        try {
            int result = (int) UPDATE_METHOD.invokeExact(state, input, length);
            if (result != 0) {
                throw new IllegalStateException("XXH64_update failed: " + result);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static long digest(MemorySegment state)
    {
        try {
            return (long) DIGEST_METHOD.invokeExact(state);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }
}
