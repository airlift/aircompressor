package io.airlift.compress;

import com.google.common.base.Throwables;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class HadoopCodecDecompressor
        implements Decompressor
{
    private final CompressionCodec codec;

    public HadoopCodecDecompressor(CompressionCodec codec)
    {
        this.codec = codec;
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        try (InputStream in = codec.createInputStream(new ByteArrayInputStream(input, inputOffset, inputLength))) {
            int bytesRead = 0;
            while (bytesRead < maxOutputLength) {
                int size = in.read(output, outputOffset + bytesRead, maxOutputLength - bytesRead);
                if (size < 0) {
                    break;
                }
                bytesRead += size;
            }

            if (in.read() >= 0) {
                throw new RuntimeException("All input was not consumed");
            }

            return bytesRead;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int decompress(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset, int maxLength)
            throws MalformedInputException
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
