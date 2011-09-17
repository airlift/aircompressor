package org.iq80.snappy;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

class UnsafeMemory implements Memory
{
    private static final Unsafe unsafe;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final int BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    @Override
    public boolean fastAccessSupported()
    {
        return true;
    }

    @Override
    public int loadInt(byte[] data, int index)
    {
        return unsafe.getInt(data, (long) (BYTE_ARRAY_OFFSET + index));
    }

    @Override
    public void copyLong(byte[] src, int srcIndex, byte[] dest, int destIndex)
    {
        long value = unsafe.getLong(src, (long) (BYTE_ARRAY_OFFSET + srcIndex));
        unsafe.putLong(dest, ((long) BYTE_ARRAY_OFFSET + destIndex), value);
    }

    @Override
    public long loadLong(byte[] data, int index)
    {
        return unsafe.getLong(data, (long) (BYTE_ARRAY_OFFSET + index));
    }

    @Override
    public void copyMemory(byte[] input, int inputIndex, byte[] output, int outputIndex, int literalLength)
    {
        unsafe.copyMemory(input, UnsafeMemory.BYTE_ARRAY_OFFSET + inputIndex, output, UnsafeMemory.BYTE_ARRAY_OFFSET + outputIndex, literalLength);
    }
}
