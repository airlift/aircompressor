package io.airlift.compress.snappy;

import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class SnappyCompressor
        implements Compressor
{
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

        return SnappyRawCompressor.compress(input, inputAddress, inputLimit, output, outputAddress, outputLimit);
    }

    @Override
    public int compress(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset, int maxOutputLength)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
