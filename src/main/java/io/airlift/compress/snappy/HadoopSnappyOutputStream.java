package io.airlift.compress.snappy;

import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

class HadoopSnappyOutputStream
        extends CompressionOutputStream
{
    public static final int DEFAULT_COMPRESSION_BLOCK = 256 * 1024;

    private static final int SIZE_OF_LONG = 8;

    private final byte[] inputBuffer;
    private final int inputMaxSize;
    private int inputOffset;

    private final byte[] outputBuffer;

    public HadoopSnappyOutputStream(OutputStream out)
    {
        this(out, DEFAULT_COMPRESSION_BLOCK);
    }

    public HadoopSnappyOutputStream(OutputStream out, int bufferSize)
    {
        super(out);
        inputBuffer = new byte[bufferSize];
        // leave extra space free at end of buffers to make compression (slightly) faster
        inputMaxSize = inputBuffer.length - SIZE_OF_LONG;
        outputBuffer = new byte[Snappy.maxCompressedLength(inputMaxSize) + SIZE_OF_LONG];
    }

    @Override
    public void write(int b)
            throws IOException
    {
        inputBuffer[inputOffset++] = (byte) b;
        if (inputOffset >= inputMaxSize) {
            writeNextChunk();
        }
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        while (length > 0) {
            int chunkSize = Math.min(length, inputMaxSize - inputOffset);
            System.arraycopy(buffer, offset, inputBuffer, inputOffset, chunkSize);
            inputOffset += chunkSize;
            length -= chunkSize;
            offset += chunkSize;

            if (inputOffset >= inputMaxSize) {
                writeNextChunk();
            }
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        if (inputOffset > 0) {
            writeNextChunk();
        }
    }

    @Override
    public void resetState()
            throws IOException
    {
        finish();
    }

    private void writeNextChunk()
            throws IOException
    {
        int compressedSize = SnappyCompressor.compress(inputBuffer, 0, inputOffset, outputBuffer, 0);

        writeBigEndianInt(inputOffset);
        writeBigEndianInt(compressedSize);
        out.write(outputBuffer, 0, compressedSize);

        inputOffset = 0;
        Arrays.fill(inputBuffer, (byte) 0);
        Arrays.fill(outputBuffer, (byte) 0);
    }

    private void writeBigEndianInt(int value)
            throws IOException
    {
        out.write(value >>> 24);
        out.write(value >>> 16);
        out.write(value >>> 8);
        out.write(value);
    }
}
