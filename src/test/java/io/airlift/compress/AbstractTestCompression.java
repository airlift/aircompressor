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
package io.airlift.compress;

import com.google.common.primitives.Bytes;
import io.airlift.compress.benchmark.DataSet;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

public abstract class AbstractTestCompression
{
    private final List<DataSet> testCases;

    public AbstractTestCompression()
    {
        testCases = new ArrayList<>();

        testCases.add(new DataSet("nothing", new byte[0]));
        testCases.add(new DataSet("short literal", "hello world!".getBytes(UTF_8)));
        testCases.add(new DataSet("small copy", "XXXXabcdabcdABCDABCDwxyzwzyz123".getBytes(UTF_8)));
        testCases.add(new DataSet("long copy", "XXXXabcdefgh abcdefgh abcdefgh abcdefgh abcdefgh abcdefgh ABC".getBytes(UTF_8)));

        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        testCases.add(new DataSet("long literal", data));

        testCases.addAll(TestingData.DATA_SETS);
    }

    protected abstract Compressor getCompressor();

    protected abstract Decompressor getDecompressor();

    protected abstract Compressor getVerifyCompressor();

    protected abstract Decompressor getVerifyDecompressor();

    protected boolean isMemorySegmentSupported()
    {
        return true;
    }

    @Test
    void testDecompress()
    {
        for (DataSet dataSet : testCases) {
            testDecompress(dataSet);
        }
    }

    void testDecompress(DataSet dataSet)
    {
        byte[] uncompressedOriginal = dataSet.getUncompressed();
        byte[] compressed = prepareCompressedData(uncompressedOriginal);

        byte[] uncompressed = new byte[uncompressedOriginal.length];

        Decompressor decompressor = getDecompressor();
        int uncompressedSize = decompressor.decompress(
                compressed,
                0,
                compressed.length,
                uncompressed,
                0,
                uncompressed.length);

        assertByteArraysEqual(uncompressed, 0, uncompressedSize, uncompressedOriginal, 0, uncompressedOriginal.length);
    }

    // Tests that decompression works correctly when the decompressed data does not span the entire output buffer
    @Test
    void testDecompressWithOutputPadding()
    {
        for (DataSet dataSet : testCases) {
            testDecompressWithOutputPadding(dataSet);
        }
    }

    private void testDecompressWithOutputPadding(DataSet dataSet)
    {
        int padding = 1021;

        byte[] uncompressedOriginal = dataSet.getUncompressed();
        byte[] compressed = prepareCompressedData(uncompressedOriginal);

        byte[] uncompressed = new byte[uncompressedOriginal.length + 2 * padding]; // pre + post padding

        Decompressor decompressor = getDecompressor();
        int uncompressedSize = decompressor.decompress(
                compressed,
                0,
                compressed.length,
                uncompressed,
                padding,
                uncompressedOriginal.length + padding);

        assertByteArraysEqual(uncompressed, padding, uncompressedSize, uncompressedOriginal, 0, uncompressedOriginal.length);
    }

    @Test
    void testDecompressionBufferOverrun()
    {
        for (DataSet dataSet : testCases) {
            testDecompressionBufferOverrun(dataSet);
        }
    }

    private void testDecompressionBufferOverrun(DataSet dataSet)
    {
        byte[] uncompressedOriginal = dataSet.getUncompressed();
        byte[] compressed = prepareCompressedData(uncompressedOriginal);

        // add padding with random bytes that we can verify later
        byte[] padding = new byte[100];
        ThreadLocalRandom.current().nextBytes(padding);

        byte[] uncompressed = Bytes.concat(new byte[uncompressedOriginal.length], padding);

        Decompressor decompressor = getDecompressor();
        int uncompressedSize = decompressor.decompress(
                compressed,
                0,
                compressed.length,
                uncompressed,
                0,
                uncompressedOriginal.length);

        assertByteArraysEqual(uncompressed, 0, uncompressedSize, uncompressedOriginal, 0, uncompressedOriginal.length);

        // verify padding is intact
        assertByteArraysEqual(padding, 0, padding.length, uncompressed, uncompressed.length - padding.length, padding.length);
    }

    @Test
    void testDecompressInputBoundsChecks()
    {
        byte[] data = new byte[1024];
        new Random(1234).nextBytes(data);
        Compressor compressor = getCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
        int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, compressed.length);

        Decompressor decompressor = getDecompressor();
        Throwable throwable;

        // null input buffer
        assertThatThrownBy(() -> decompressor.decompress(null, 0, compressedLength, data, 0, data.length))
                .isInstanceOf(NullPointerException.class);

        // mis-declared buffer size
        byte[] compressedChoppedOff = Arrays.copyOf(compressed, compressedLength - 1);
        throwable = catchThrowable(() -> decompressor.decompress(compressedChoppedOff, 0, compressedLength, data, 0, data.length));
        if (throwable instanceof UncheckedIOException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }

        // overrun because of offset
        byte[] compressedWithPadding = new byte[10 + compressedLength - 1];
        arraycopy(compressed, 0, compressedWithPadding, 10, compressedLength - 1);

        throwable = catchThrowable(() -> decompressor.decompress(compressedWithPadding, 10, compressedLength, data, 0, data.length));
        if (throwable instanceof UncheckedIOException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }
    }

    @Test
    void testDecompressOutputBoundsChecks()
    {
        byte[] data = new byte[1024];
        new Random(1234).nextBytes(data);
        Compressor compressor = getCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
        int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, compressed.length);
        byte[] input = Arrays.copyOf(compressed, compressedLength);

        Decompressor decompressor = getDecompressor();
        Throwable throwable;

        // null output buffer
        assertThatThrownBy(() -> decompressor.decompress(input, 0, input.length, null, 0, data.length))
                .isInstanceOf(NullPointerException.class);

        // small buffer
        assertThatThrownBy(() -> decompressor.decompress(input, 0, input.length, new byte[1], 0, 1))
                .hasMessageMatching("All input was not consumed|attempt to write.* outside of destination buffer.*|Malformed input.*|Uncompressed length 1024 must be less than 1|Output buffer too small.*");

        // mis-declared buffer size
        throwable = catchThrowable(() -> decompressor.decompress(input, 0, input.length, new byte[1], 0, data.length));
        if (throwable instanceof IndexOutOfBoundsException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }

        // mis-declared buffer size with greater buffer
        throwable = catchThrowable(() -> decompressor.decompress(input, 0, input.length, new byte[data.length - 1], 0, data.length));
        if (throwable instanceof IndexOutOfBoundsException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }
    }

    @Test
    void testDecompressMemorySegmentHeapToHeap()
    {
        for (DataSet dataSet : testCases) {
            testDecompressMemorySegmentHeapToHeap(dataSet);
        }
    }

    void testDecompressMemorySegmentHeapToHeap(DataSet dataSet)
    {
        if (!isMemorySegmentSupported()) {
            Assumptions.abort("MemorySegment not supported");
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        MemorySegment compressed = MemorySegment.ofArray(prepareCompressedData(uncompressedOriginal));
        MemorySegment uncompressed = MemorySegment.ofArray(new byte[uncompressedOriginal.length]);

        int size = getDecompressor().decompress(compressed, uncompressed);
        assertMemorySegmentEqual(MemorySegment.ofArray(uncompressedOriginal), uncompressed.asSlice(0, size));
    }

    @Test
    void testDecompressMemorySegmentHeapToDirect()
    {
        for (DataSet dataSet : testCases) {
            testDecompressMemorySegmentHeapToDirect(dataSet);
        }
    }

    private void testDecompressMemorySegmentHeapToDirect(DataSet dataSet)
    {
        if (!isMemorySegmentSupported()) {
            Assumptions.abort("MemorySegment not supported");
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment compressed = MemorySegment.ofArray(prepareCompressedData(uncompressedOriginal));
            MemorySegment uncompressed = arena.allocate(uncompressedOriginal.length);

            int size = getDecompressor().decompress(compressed, uncompressed);
            assertMemorySegmentEqual(MemorySegment.ofArray(uncompressedOriginal), uncompressed.asSlice(0, size));
        }
    }

    @Test
    void testDecompressMemorySegmentDirectToHeap()
    {
        for (DataSet dataSet : testCases) {
            testDecompressMemorySegmentDirectToHeap(dataSet);
        }
    }

    private void testDecompressMemorySegmentDirectToHeap(DataSet dataSet)
    {
        if (!isMemorySegmentSupported()) {
            Assumptions.abort("MemorySegment not supported");
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment compressed = toNativeSegment(arena, prepareCompressedData(uncompressedOriginal));
            MemorySegment uncompressed = MemorySegment.ofArray(new byte[uncompressedOriginal.length]);

            int size = getDecompressor().decompress(compressed, uncompressed);
            assertMemorySegmentEqual(MemorySegment.ofArray(uncompressedOriginal), uncompressed.asSlice(0, size));
        }
    }

    @Test
    void testDecompressMemorySegmentDirectToDirect()
    {
        for (DataSet dataSet : testCases) {
            testDecompressMemorySegmentDirectToDirect(dataSet);
        }
    }

    private void testDecompressMemorySegmentDirectToDirect(DataSet dataSet)
    {
        if (!isMemorySegmentSupported()) {
            Assumptions.abort("MemorySegment not supported");
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment compressed = toNativeSegment(arena, prepareCompressedData(uncompressedOriginal));
            MemorySegment uncompressed = arena.allocate(uncompressedOriginal.length);

            int size = getDecompressor().decompress(compressed, uncompressed);
            assertMemorySegmentEqual(MemorySegment.ofArray(uncompressedOriginal), uncompressed.asSlice(0, size));
        }
    }

    @Test
    void testCompress()
    {
        for (DataSet dataSet : testCases) {
            testCompress(dataSet);
        }
    }

    private void testCompress(DataSet testCase)
    {
        Compressor compressor = getCompressor();

        byte[] originalUncompressed = testCase.getUncompressed();
        byte[] compressed = new byte[compressor.maxCompressedLength(originalUncompressed.length)];

        // attempt to compress slightly different data to ensure the compressor doesn't keep state
        // between calls that may affect results
        if (originalUncompressed.length > 1) {
            byte[] output = new byte[compressor.maxCompressedLength(originalUncompressed.length - 1)];
            compressor.compress(originalUncompressed, 1, originalUncompressed.length - 1, output, 0, output.length);
        }

        int compressedLength = compressor.compress(
                originalUncompressed,
                0,
                originalUncompressed.length,
                compressed,
                0,
                compressed.length);

        verifyCompressedData(originalUncompressed, compressed, compressedLength);
    }

    @Test
    void testCompressInputBoundsChecks()
    {
        Compressor compressor = getCompressor();
        int declaredInputLength = 1024;
        int maxCompressedLength = compressor.maxCompressedLength(1024);
        byte[] output = new byte[maxCompressedLength];
        Throwable throwable;

        // null input buffer
        assertThatThrownBy(() -> compressor.compress(null, 0, declaredInputLength, output, 0, output.length))
                .isInstanceOf(NullPointerException.class);

        // mis-declared buffer size
        throwable = catchThrowable(() -> compressor.compress(new byte[1], 0, declaredInputLength, output, 0, output.length));
        if (throwable instanceof IndexOutOfBoundsException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }

        // max too small
        throwable = catchThrowable(() -> compressor.compress(new byte[declaredInputLength - 1], 0, declaredInputLength, output, 0, output.length));
        if (throwable instanceof IndexOutOfBoundsException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }

        // overrun because of offset
        throwable = catchThrowable(() -> compressor.compress(new byte[declaredInputLength + 10], 11, declaredInputLength, output, 0, output.length));
        if (throwable instanceof IndexOutOfBoundsException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }
    }

    @Test
    void testCompressOutputBoundsChecks()
    {
        Compressor compressor = getCompressor();
        int minCompressionOverhead = compressor.maxCompressedLength(0);
        byte[] input = new byte[minCompressionOverhead * 4 + 1024];
        new Random(1234).nextBytes(input);
        int maxCompressedLength = compressor.maxCompressedLength(input.length);
        Throwable throwable;

        // null output buffer
        assertThatThrownBy(() -> compressor.compress(input, 0, input.length, null, 0, maxCompressedLength))
                .isInstanceOf(NullPointerException.class);

        // small buffer
        assertThatThrownBy(() -> compressor.compress(input, 0, input.length, new byte[1], 0, 1))
                .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*|Max output length must be larger than .*|Output buffer must be at least.*|Output buffer too small");

        // mis-declared buffer size
        throwable = catchThrowable(() -> compressor.compress(input, 0, input.length, new byte[1], 0, maxCompressedLength));
        if (throwable instanceof ArrayIndexOutOfBoundsException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }

        // mis-declared buffer size with buffer large enough to hold compression frame header (if any)
        throwable = catchThrowable(() -> compressor.compress(input, 0, input.length, new byte[minCompressionOverhead * 2], 0, maxCompressedLength));
        if (throwable instanceof ArrayIndexOutOfBoundsException) {
            // OK
        }
        else {
            assertThat(throwable)
                    .hasMessageMatching(".*must not be greater than size.*|Invalid offset or length.*");
        }
    }

    @Test
    void testCompressMemorySegmentHeapToHeap()
    {
        for (DataSet dataSet : testCases) {
            testCompressMemorySegmentHeapToHeap(dataSet);
        }
    }

    private void testCompressMemorySegmentHeapToHeap(DataSet dataSet)
    {
        if (!isMemorySegmentSupported()) {
            Assumptions.abort("MemorySegment not supported");
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        Compressor compressor = getCompressor();

        verifyCompressMemorySegment(
                compressor,
                MemorySegment.ofArray(uncompressedOriginal),
                MemorySegment.ofArray(new byte[compressor.maxCompressedLength(uncompressedOriginal.length)]));
    }

    @Test
    void testCompressMemorySegmentHeapToDirect()
    {
        for (DataSet dataSet : testCases) {
            testCompressMemorySegmentHeapToDirect(dataSet);
        }
    }

    private void testCompressMemorySegmentHeapToDirect(DataSet dataSet)
    {
        if (!isMemorySegmentSupported()) {
            Assumptions.abort("MemorySegment not supported");
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        Compressor compressor = getCompressor();

        try (Arena arena = Arena.ofConfined()) {
            verifyCompressMemorySegment(
                    compressor,
                    MemorySegment.ofArray(uncompressedOriginal),
                    arena.allocate(compressor.maxCompressedLength(uncompressedOriginal.length)));
        }
    }

    @Test
    void testCompressMemorySegmentDirectToHeap()
    {
        for (DataSet dataSet : testCases) {
            testCompressMemorySegmentDirectToHeap(dataSet);
        }
    }

    private void testCompressMemorySegmentDirectToHeap(DataSet dataSet)
    {
        if (!isMemorySegmentSupported()) {
            Assumptions.abort("MemorySegment not supported");
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        Compressor compressor = getCompressor();

        try (Arena arena = Arena.ofConfined()) {
            verifyCompressMemorySegment(
                    compressor,
                    toNativeSegment(arena, uncompressedOriginal),
                    MemorySegment.ofArray(new byte[compressor.maxCompressedLength(uncompressedOriginal.length)]));
        }
    }

    @Test
    void testCompressMemorySegmentDirectToDirect()
    {
        for (DataSet dataSet : testCases) {
            testCompressMemorySegmentDirectToDirect(dataSet);
        }
    }

    private void testCompressMemorySegmentDirectToDirect(DataSet dataSet)
    {
        if (!isMemorySegmentSupported()) {
            Assumptions.abort("MemorySegment not supported");
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        Compressor compressor = getCompressor();

        try (Arena arena = Arena.ofConfined()) {
            verifyCompressMemorySegment(
                    compressor,
                    toNativeSegment(arena, uncompressedOriginal),
                    arena.allocate(compressor.maxCompressedLength(uncompressedOriginal.length)));
        }
    }

    private void verifyCompressMemorySegment(Compressor compressor, MemorySegment expected, MemorySegment compressed)
    {
        // attempt to compress slightly different data to ensure the compressor doesn't keep state
        // between calls that may affect results
        if (expected.byteSize() > 1) {
            compressor.compress(expected.asSlice(1), MemorySegment.ofArray(new byte[(int) compressed.byteSize()]));
        }

        int compressedSize = compressor.compress(expected, compressed);

        MemorySegment uncompressed = MemorySegment.ofArray(new byte[(int) expected.byteSize()]);

        int decompressedSize = getDecompressor().decompress(compressed.asSlice(0, compressedSize), uncompressed);

        assertMemorySegmentEqual(expected, uncompressed.asSlice(0, decompressedSize));
    }

    private void verifyCompressedData(byte[] originalUncompressed, byte[] compressed, int compressedLength)
    {
        byte[] uncompressed = new byte[originalUncompressed.length];
        int uncompressedSize = getVerifyDecompressor().decompress(compressed, 0, compressedLength, uncompressed, 0, uncompressed.length);

        assertByteArraysEqual(uncompressed, 0, uncompressedSize, originalUncompressed, 0, originalUncompressed.length);
    }

    @Test
    void testRoundTripSmallLiteral()
    {
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        Compressor compressor = getCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(data.length)];
        byte[] uncompressed = new byte[data.length];

        for (int i = 1; i < data.length; i++) {
            try {
                int written = compressor.compress(
                        data,
                        0,
                        i,
                        compressed,
                        0,
                        compressed.length);

                int decompressedSize = getDecompressor().decompress(compressed, 0, written, uncompressed, 0, uncompressed.length);

                assertByteArraysEqual(data, 0, i, uncompressed, 0, decompressedSize);
                assertThat(decompressedSize).isEqualTo(i);
            }
            catch (MalformedInputException e) {
                throw new RuntimeException("Failed with " + i + " bytes of input", e);
            }
        }
    }

    protected static void assertByteArraysEqual(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        checkPositionIndexes(leftOffset, leftOffset + leftLength, left.length);
        checkPositionIndexes(rightOffset, rightOffset + rightLength, right.length);

        for (int i = 0; i < Math.min(leftLength, rightLength); i++) {
            if (left[leftOffset + i] != right[rightOffset + i]) {
                throw new AssertionError(format("Byte arrays differ at position %s: 0x%02X vs 0x%02X", i, left[leftOffset + i], right[rightOffset + i]));
            }
        }

        assertThat(leftLength)
                .withFailMessage(format("Array lengths differ: %s vs %s", leftLength, rightLength))
                .isEqualTo(rightLength);
    }

    private static void assertMemorySegmentEqual(MemorySegment left, MemorySegment right)
    {
        int leftPosition = 0;
        int rightPosition = 0;
        for (int i = 0; i < Math.min(left.byteSize(), right.byteSize()); i++) {
            byte leftByte = left.get(JAVA_BYTE, leftPosition + i);
            byte rightByte = right.get(JAVA_BYTE, rightPosition + i);
            if (leftByte != rightByte) {
                throw new AssertionError("Byte buffers differ at position %s: 0x%02X vs 0x%02X".formatted(i, leftByte, rightPosition));
            }
        }
        assertThat(left.byteSize())
                .withFailMessage("Buffer lengths differ: %s vs %s".formatted(left.byteSize(), right.byteSize()))
                .isEqualTo(right.byteSize());
    }

    private static MemorySegment toNativeSegment(Arena arena, byte[] data)
    {
        return arena.allocateFrom(JAVA_BYTE, data);
    }

    private byte[] prepareCompressedData(byte[] uncompressed)
    {
        Compressor compressor = getVerifyCompressor();

        byte[] compressed = new byte[compressor.maxCompressedLength(uncompressed.length)];

        int compressedLength = compressor.compress(
                uncompressed,
                0,
                uncompressed.length,
                compressed,
                0,
                compressed.length);

        return Arrays.copyOf(compressed, compressedLength);
    }
}
