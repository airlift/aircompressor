package org.iq80.snappy;

import com.google.common.base.Preconditions;

import java.io.OutputStream;
import java.util.Arrays;

final public class ByteArrayOutputStream extends OutputStream
{

    private final byte buffer[];
    private int size;

    public ByteArrayOutputStream(int size)
    {
        this(new byte[size]);
    }

    public ByteArrayOutputStream(byte[] buffer)
    {
        this.buffer = buffer;
    }

    public void write(int b)
    {
        Preconditions.checkPositionIndex(size + 1, buffer.length);
        buffer[size++] = (byte) b;
    }

    public void write(byte b[], int off, int len)
    {
        Preconditions.checkPositionIndex(size + len, buffer.length);
        System.arraycopy(b, off, buffer, size, len);
        size += len;
    }

    public void reset()
    {
        size = 0;
    }

    public int size()
    {
        return size;
    }

    public byte[] getBuffer()
    {
        return buffer;
    }

    public byte[] toByteArray()
    {
        return Arrays.copyOf(buffer, size);
    }
}
