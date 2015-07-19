package io.airlift.compress.snappy;

import io.airlift.compress.Compressor;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class SnappyCompressor
        implements Compressor
{
    private final short[] table = new short[SnappyRawCompressor.MAX_HASH_TABLE_SIZE];

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return SnappyRawCompressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long inputLimit = inputAddress + inputLength;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;
        long outputLimit = outputAddress + maxOutputLength;

        return SnappyRawCompressor.compress(input, inputAddress, inputLimit, output, outputAddress, outputLimit, table);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        Object inputBase;
        long inputAddress;
        long inputLimit;
        if (input instanceof DirectBuffer) {
            DirectBuffer direct = (DirectBuffer) input;
            inputBase = null;
            inputAddress = direct.address() + input.position();
            inputLimit = direct.address() + input.limit();
        }
        else if (input.hasArray()) {
            inputBase = input.array();
            inputAddress = ARRAY_BYTE_BASE_OFFSET + input.arrayOffset() + input.position();
            inputLimit = ARRAY_BYTE_BASE_OFFSET + input.arrayOffset() + input.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported input ByteBuffer implementation " + input.getClass().getName());
        }

        Object outputBase;
        long outputAddress;
        long outputLimit;
        if (output instanceof DirectBuffer) {
            DirectBuffer direct = (DirectBuffer) output;
            outputBase = null;
            outputAddress = direct.address() + output.position();
            outputLimit = direct.address() + output.limit();
        }
        else if (output.hasArray()) {
            outputBase = output.array();
            outputAddress = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.position();
            outputLimit = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported output ByteBuffer implementation " + output.getClass().getName());
        }

        // HACK: Assure JVM does not collect Slice wrappers while compressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault
        // There is no other known way to signal to the JVM that an object should not be
        // collected in a block, and technically, the JVM is allowed to eliminate these locks.
        synchronized (input) {
            synchronized (output) {
                int written = SnappyRawCompressor.compress(
                        inputBase,
                        inputAddress,
                        inputLimit,
                        outputBase,
                        outputAddress,
                        outputLimit,
                        table);
                output.position(output.position() + written);
            }
        }
    }
}
