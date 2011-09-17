package org.iq80.snappy;

class SlowMemory implements Memory
{
    @Override
    public boolean fastAccessSupported()
    {
        return false;
    }

    @Override
    public int loadInt(byte[] data, int index)
    {
        return (data[index] & 0xff) |
                (data[index + 1] & 0xff) << 8 |
                (data[index + 2] & 0xff) << 16 |
                (data[index + 3] & 0xff) << 24;
    }

    @Override
    public void copyLong(byte[] src, int srcIndex, byte[] dest, int destIndex)
    {
        for (int i = 0; i < 8; i++) {
            dest[destIndex + i] = src[srcIndex + i];
        }
    }

    @Override
    public long loadLong(byte[] data, int index)
    {
        return (data[index] & 0xffL) |
                (data[index + 1] & 0xffL) << 8 |
                (data[index + 2] & 0xffL) << 16 |
                (data[index + 3] & 0xffL) << 24 |
                (data[index + 4] & 0xffL) << 32 |
                (data[index + 5] & 0xffL) << 40 |
                (data[index + 6] & 0xffL) << 48 |
                (data[index + 7] & 0xffL) << 56;
    }

    @Override
    public void copyMemory(byte[] input, int inputIndex, byte[] output, int outputIndex, int length)
    {
        System.arraycopy(input, inputIndex, output, outputIndex, length);
    }
}
