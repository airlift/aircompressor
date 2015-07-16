package io.airlift.compress.snappy;

import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;

public class SnappyCompressor
        implements Compressor
{
    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return Snappy.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        return SnappyRawCompressor.compress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public int compress(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset, int maxOutputLength)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
