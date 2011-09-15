package org.iq80.snappy;

interface Memory
{
    boolean fastAccessSupported();

    int loadInt(byte[] data, int index);

    void copyLong(byte[] src, int srcIndex, byte[] dest, int destIndex);

    long loadLong(byte[] data, int index);
}
