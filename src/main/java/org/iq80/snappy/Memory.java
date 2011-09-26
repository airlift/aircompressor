package org.iq80.snappy;

interface Memory
{
    boolean fastAccessSupported();

    int lookupShort(short[] data, int index);

    int loadByte(byte[] data, int index);

    int loadInt(byte[] data, int index);

    void copyLong(byte[] src, int srcIndex, byte[] dest, int destIndex);

    long loadLong(byte[] data, int index);

    void copyMemory(byte[] input, int inputIndex, byte[] output, int outputIndex, int length);
}
