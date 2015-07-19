package io.airlift.compress;

import com.google.common.base.Throwables;
import io.airlift.compress.snappy.ByteArrayOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class HadoopCodecCompressor
        implements Compressor
{
    private final CompressionCodec codec;
    private final Compressor blockCompressorForSizeCalculation;

    public HadoopCodecCompressor(CompressionCodec codec, Compressor blockCompressorForSizeCalculation)
    {
        this.codec = codec;
        this.blockCompressorForSizeCalculation = blockCompressorForSizeCalculation;
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        // assume hadoop stream encoder won't increase size by more than 10% over the block encoder
        return (int) ((blockCompressorForSizeCalculation.maxCompressedLength(uncompressedSize) + 1.1) + 8);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(output, outputOffset, maxOutputLength);

        try {
            OutputStream out = codec.createOutputStream(byteArrayOutputStream);
            // write in a single shot to cause multiple chunks per block
            out.write(input, inputOffset, inputLength);
            out.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return byteArrayOutputStream.size();
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
