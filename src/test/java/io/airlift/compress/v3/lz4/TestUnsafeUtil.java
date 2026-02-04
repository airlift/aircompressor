package io.airlift.compress.v3.lz4;

import org.junit.jupiter.api.Test;
import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestUnsafeUtil {
    @Test
    void testGetBaseNative() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment memorySegment = arena.allocate(10);
            assertThat(UnsafeUtil.getBase(memorySegment)).isNull();
        }
    }

    @Test
    void testGetBaseArray() {
        byte[] array = new byte[10];
        MemorySegment memorySegment = MemorySegment.ofArray(array);
        assertThat(UnsafeUtil.getBase(memorySegment)).isSameAs(array);
    }

    @Test
    void testGetBaseReadOnly() {
        byte[] array = new byte[10];
        MemorySegment memorySegment = MemorySegment.ofArray(array).asReadOnly();
        assertThatThrownBy(() -> UnsafeUtil.getBase(memorySegment))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("MemorySegment is read-only");
    }

    @Test
    void testGetBaseNonByteArray() {
        long[] array = new long[10];
        MemorySegment memorySegment = MemorySegment.ofArray(array);
        assertThatThrownBy(() -> UnsafeUtil.getBase(memorySegment))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("MemorySegment is not backed by a byte array");
    }

    @Test
    void testGetAddressNative() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment memorySegment = arena.allocate(10);
            assertThat(UnsafeUtil.getAddress(memorySegment)).isEqualTo(memorySegment.address());
        }
    }

    @Test
    void testGetAddressArray() {
        byte[] array = new byte[10];
        MemorySegment memorySegment = MemorySegment.ofArray(array);
        assertThat(UnsafeUtil.getAddress(memorySegment)).isEqualTo(Unsafe.ARRAY_BYTE_BASE_OFFSET);

        // Create a segment backed by a byte[] which does not start at index 0
        SegmentAllocator allocator = SegmentAllocator.slicingAllocator(memorySegment);
        allocator.allocate(5);  // ignore result
        memorySegment = allocator.allocate(3);
        assertThat(UnsafeUtil.getAddress(memorySegment)).isEqualTo(Unsafe.ARRAY_BYTE_BASE_OFFSET + 5);
    }

    @Test
    void testGetAddressInaccessible() throws Exception {
        // Obtain MemorySegment with confined scope from other thread
        CountDownLatch hasSegment = new CountDownLatch(1);
        CountDownLatch canClose = new CountDownLatch(1);
        AtomicReference<MemorySegment> memorySegmentRef = new AtomicReference<>();
        Thread.ofPlatform().daemon().start(() -> {
            Arena arena = Arena.ofConfined();
            memorySegmentRef.set(arena.allocate(10));
            hasSegment.countDown();

            // Wait with resource clean-up until main thread has used memory segment
            try {
                canClose.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            arena.close();
        });

        hasSegment.await();
        assertThatThrownBy(() -> UnsafeUtil.getAddress(memorySegmentRef.get()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("MemorySegment is not accessible by current thread");

        canClose.countDown();
    }

    @Test
    void testGetAddressNotAlive() {
        MemorySegment memorySegment;
        // Closes Arena before segment is used
        try (Arena arena = Arena.ofConfined()) {
            memorySegment = arena.allocate(10);
        }

        assertThatThrownBy(() -> UnsafeUtil.getAddress(memorySegment))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("MemorySegment scope is not alive");
    }

    @Test
    void testGetAddressNonByteArray() {
        long[] array = new long[10];
        MemorySegment memorySegment = MemorySegment.ofArray(array);
        assertThatThrownBy(() -> UnsafeUtil.getAddress(memorySegment))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("MemorySegment is not backed by a byte array");
    }
}
