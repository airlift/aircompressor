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

import io.airlift.compress.v3.internal.NativeBuffers;
import io.airlift.compress.v3.internal.NativeLoader;
import io.airlift.compress.v3.internal.NativeLoader.Symbols;
import io.airlift.compress.v3.internal.NativeSignature;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Optional;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.invoke.MethodHandles.lookup;

final class XxHash3Bindings
{
    private static final VarHandle LONG_HANDLE = java.lang.invoke.MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    // Reusable per-thread scratch for 16-byte struct returns, so hash128/digest128 avoid allocating
    // a byte[16] and a capturing SegmentAllocator on every call.
    private record Struct128(byte[] bytes, SegmentAllocator allocator) {}

    private static final ThreadLocal<Struct128> STRUCT_128 = ThreadLocal.withInitial(() -> {
        byte[] bytes = new byte[16];
        MemorySegment segment = MemorySegment.ofArray(bytes);
        return new Struct128(bytes, (_, _) -> segment);
    });

    private XxHash3Bindings() {}

    // Record for functions that can use @NativeSignature (primitives and MemorySegment only)
    private record MethodHandles(
            // One-shot 64-bit hash
            @NativeSignature(critical = true, name = "XXH3_64bits", returnType = long.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle hash64Critical,
            @NativeSignature(name = "XXH3_64bits", returnType = long.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle hash64,
            @NativeSignature(critical = true, name = "XXH3_64bits_withSeed", returnType = long.class, argumentTypes = {MemorySegment.class, long.class, long.class})
            MethodHandle hash64WithSeedCritical,
            @NativeSignature(name = "XXH3_64bits_withSeed", returnType = long.class, argumentTypes = {MemorySegment.class, long.class, long.class})
            MethodHandle hash64WithSeed,
            // State management
            @NativeSignature(name = "XXH3_createState", returnType = MemorySegment.class, argumentTypes = {})
            MethodHandle createState,
            @NativeSignature(name = "XXH3_freeState", returnType = int.class, argumentTypes = MemorySegment.class)
            MethodHandle freeState,
            // 64-bit streaming
            @NativeSignature(name = "XXH3_64bits_reset_withSeed", returnType = int.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle reset64WithSeed,
            @NativeSignature(critical = true, name = "XXH3_64bits_update", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class})
            MethodHandle update64Critical,
            @NativeSignature(name = "XXH3_64bits_update", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class})
            MethodHandle update64,
            @NativeSignature(name = "XXH3_64bits_digest", returnType = long.class, argumentTypes = MemorySegment.class)
            MethodHandle digest64,
            // 128-bit streaming (reset and update)
            @NativeSignature(name = "XXH3_128bits_reset_withSeed", returnType = int.class, argumentTypes = {MemorySegment.class, long.class})
            MethodHandle reset128WithSeed,
            @NativeSignature(critical = true, name = "XXH3_128bits_update", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class})
            MethodHandle update128Critical,
            @NativeSignature(name = "XXH3_128bits_update", returnType = int.class, argumentTypes = {MemorySegment.class, MemorySegment.class, long.class})
            MethodHandle update128) {}

    // Struct layout for XXH128_hash_t: { uint64_t low64; uint64_t high64; }
    private static final StructLayout XXH128_HASH_STRUCT = MemoryLayout.structLayout(
            JAVA_LONG.withName("low64"),
            JAVA_LONG.withName("high64"));

    private static final Optional<LinkageError> LINKAGE_ERROR;

    // One-shot hash methods
    private static final MethodHandle HASH64_METHOD;
    private static final MethodHandle HASH64_CRITICAL_METHOD;
    private static final MethodHandle HASH64_WITH_SEED_METHOD;
    private static final MethodHandle HASH64_WITH_SEED_CRITICAL_METHOD;
    private static final MethodHandle HASH128_METHOD;
    private static final MethodHandle HASH128_NONCRITICAL_METHOD;
    private static final MethodHandle HASH128_WITH_SEED_METHOD;
    private static final MethodHandle HASH128_WITH_SEED_NONCRITICAL_METHOD;

    // State management methods
    private static final MethodHandle CREATE_STATE_METHOD;
    private static final MethodHandle FREE_STATE_METHOD;

    // 64-bit streaming methods
    private static final MethodHandle RESET64_WITH_SEED_METHOD;
    private static final MethodHandle UPDATE64_METHOD;
    private static final MethodHandle UPDATE64_CRITICAL_METHOD;
    private static final MethodHandle DIGEST64_METHOD;

    // 128-bit streaming methods
    private static final MethodHandle RESET128_WITH_SEED_METHOD;
    private static final MethodHandle UPDATE128_METHOD;
    private static final MethodHandle UPDATE128_CRITICAL_METHOD;
    private static final MethodHandle DIGEST128_METHOD;

    static {
        Symbols<MethodHandles> symbols = NativeLoader.loadSymbols("xxhash", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        MethodHandles methodHandles = symbols.symbols();

        // One-shot hash
        HASH64_METHOD = methodHandles.hash64();
        HASH64_CRITICAL_METHOD = methodHandles.hash64Critical();
        HASH64_WITH_SEED_METHOD = methodHandles.hash64WithSeed();
        HASH64_WITH_SEED_CRITICAL_METHOD = methodHandles.hash64WithSeedCritical();

        // State management
        CREATE_STATE_METHOD = methodHandles.createState();
        FREE_STATE_METHOD = methodHandles.freeState();

        // 64-bit streaming
        RESET64_WITH_SEED_METHOD = methodHandles.reset64WithSeed();
        UPDATE64_METHOD = methodHandles.update64();
        UPDATE64_CRITICAL_METHOD = methodHandles.update64Critical();
        DIGEST64_METHOD = methodHandles.digest64();

        // 128-bit streaming
        RESET128_WITH_SEED_METHOD = methodHandles.reset128WithSeed();
        UPDATE128_METHOD = methodHandles.update128();
        UPDATE128_CRITICAL_METHOD = methodHandles.update128Critical();

        // Manual setup for functions that return a struct
        if (LINKAGE_ERROR.isEmpty()) {
            try {
                SymbolLookup symbolLookup = NativeLoader.loadLibrary("xxhash");

                HASH128_NONCRITICAL_METHOD = structMethod(symbolLookup, "XXH3_128bits", false, ADDRESS, JAVA_LONG);
                HASH128_METHOD = structMethod(symbolLookup, "XXH3_128bits", true, ADDRESS, JAVA_LONG);
                HASH128_WITH_SEED_NONCRITICAL_METHOD = structMethod(symbolLookup, "XXH3_128bits_withSeed", false, ADDRESS, JAVA_LONG, JAVA_LONG);
                HASH128_WITH_SEED_METHOD = structMethod(symbolLookup, "XXH3_128bits_withSeed", true, ADDRESS, JAVA_LONG, JAVA_LONG);
                DIGEST128_METHOD = structMethod(symbolLookup, "XXH3_128bits_digest", true, ADDRESS);
            }
            catch (LinkageError e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        else {
            HASH128_METHOD = null;
            HASH128_NONCRITICAL_METHOD = null;
            HASH128_WITH_SEED_METHOD = null;
            HASH128_WITH_SEED_NONCRITICAL_METHOD = null;
            DIGEST128_METHOD = null;
        }
    }

    private static MethodHandle structMethod(SymbolLookup symbols, String name, boolean critical, MemoryLayout... arguments)
    {
        MemorySegment address = symbols.find(name).orElseThrow(() -> new LinkageError("unresolved symbol: " + name));
        FunctionDescriptor descriptor = FunctionDescriptor.of(XXH128_HASH_STRUCT, arguments);
        Linker linker = Linker.nativeLinker();
        return critical ? linker.downcallHandle(address, descriptor, Linker.Option.critical(true)) : linker.downcallHandle(address, descriptor);
    }

    public static boolean isEnabled()
    {
        return LINKAGE_ERROR.isEmpty();
    }

    public static void verifyEnabled()
    {
        if (LINKAGE_ERROR.isPresent()) {
            throw new IllegalStateException("XxHash3 native library is not enabled", LINKAGE_ERROR.get());
        }
    }

    // One-shot hash methods

    public static long hash64(MemorySegment input, long length)
    {
        try {
            if (length <= 8192) {
                return (long) HASH64_CRITICAL_METHOD.invokeExact(input, length);
            }
            try (NativeBuffers buffers = new NativeBuffers()) {
                return (long) HASH64_METHOD.invokeExact(buffers.input(input, length), length);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static long hash64(MemorySegment input, long length, long seed)
    {
        try {
            if (length <= 8192) {
                return (long) HASH64_WITH_SEED_CRITICAL_METHOD.invokeExact(input, length, seed);
            }
            try (NativeBuffers buffers = new NativeBuffers()) {
                return (long) HASH64_WITH_SEED_METHOD.invokeExact(buffers.input(input, length), length, seed);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static XxHash128 hash128(MemorySegment input, long length)
    {
        try {
            if (length <= 8192) {
                Struct128 scratch = STRUCT_128.get();
                MemorySegment _ = (MemorySegment) HASH128_METHOD.invokeExact(scratch.allocator(), input, length);
                byte[] bytes = scratch.bytes();
                return new XxHash128((long) LONG_HANDLE.get(bytes, 0), (long) LONG_HANDLE.get(bytes, 8));
            }
            try (NativeBuffers buffers = new NativeBuffers()) {
                SegmentAllocator allocator = buffers.arena();
                MemorySegment result = (MemorySegment) HASH128_NONCRITICAL_METHOD.invokeExact(allocator, buffers.input(input, length), length);
                return new XxHash128(result.get(JAVA_LONG, 0), result.get(JAVA_LONG, Long.BYTES));
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static XxHash128 hash128(MemorySegment input, long length, long seed)
    {
        try {
            if (length <= 8192) {
                Struct128 scratch = STRUCT_128.get();
                MemorySegment _ = (MemorySegment) HASH128_WITH_SEED_METHOD.invokeExact(scratch.allocator(), input, length, seed);
                byte[] bytes = scratch.bytes();
                return new XxHash128((long) LONG_HANDLE.get(bytes, 0), (long) LONG_HANDLE.get(bytes, 8));
            }
            try (NativeBuffers buffers = new NativeBuffers()) {
                SegmentAllocator allocator = buffers.arena();
                MemorySegment result = (MemorySegment) HASH128_WITH_SEED_NONCRITICAL_METHOD.invokeExact(allocator, buffers.input(input, length), length, seed);
                return new XxHash128(result.get(JAVA_LONG, 0), result.get(JAVA_LONG, Long.BYTES));
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    // State management

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
                throw new IllegalStateException("XXH3_freeState failed: " + result);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    // 64-bit streaming

    public static void reset64(MemorySegment state, long seed)
    {
        try {
            int result = (int) RESET64_WITH_SEED_METHOD.invokeExact(state, seed);
            if (result != 0) {
                throw new IllegalStateException("XXH3_64bits_reset_withSeed failed: " + result);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static void update64(MemorySegment state, MemorySegment input, long length)
    {
        try {
            int result;
            if (length <= 8192) {
                result = (int) UPDATE64_CRITICAL_METHOD.invokeExact(state, input, length);
            }
            else {
                try (NativeBuffers buffers = new NativeBuffers()) {
                    result = (int) UPDATE64_METHOD.invokeExact(state, buffers.input(input, length), length);
                }
            }
            if (result != 0) {
                throw new IllegalStateException("XXH3_64bits_update failed: " + result);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static long digest64(MemorySegment state)
    {
        try {
            return (long) DIGEST64_METHOD.invokeExact(state);
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    // 128-bit streaming

    public static void reset128(MemorySegment state, long seed)
    {
        try {
            int result = (int) RESET128_WITH_SEED_METHOD.invokeExact(state, seed);
            if (result != 0) {
                throw new IllegalStateException("XXH3_128bits_reset_withSeed failed: " + result);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static void update128(MemorySegment state, MemorySegment input, long length)
    {
        try {
            int result;
            if (length <= 8192) {
                result = (int) UPDATE128_CRITICAL_METHOD.invokeExact(state, input, length);
            }
            else {
                try (NativeBuffers buffers = new NativeBuffers()) {
                    result = (int) UPDATE128_METHOD.invokeExact(state, buffers.input(input, length), length);
                }
            }
            if (result != 0) {
                throw new IllegalStateException("XXH3_128bits_update failed: " + result);
            }
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static XxHash128 digest128(MemorySegment state)
    {
        try {
            Struct128 scratch = STRUCT_128.get();
            MemorySegment _ = (MemorySegment) DIGEST128_METHOD.invokeExact(scratch.allocator(), state);
            byte[] bytes = scratch.bytes();
            return new XxHash128((long) LONG_HANDLE.get(bytes, 0), (long) LONG_HANDLE.get(bytes, 8));
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }
}
