package org.iq80.snappy;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

final class SnappyInternalUtils
{
    private SnappyInternalUtils()
    {
    }

    static final boolean HAS_UNSAFE = true;
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

    static final long BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    static boolean equals(byte[] left, int leftIndex, byte[] right, int rightIndex, int length)
    {
        checkPositionIndexes(leftIndex, leftIndex + length, left.length);
        checkPositionIndexes(rightIndex, rightIndex + length, right.length);

        for (int i = 0; i < length; i++) {
            if (left[leftIndex + i] != right[rightIndex + i]) {
                return false;
            }
        }
        return true;
    }


    static int loadInt(byte[] data, int index)
    {
        if (HAS_UNSAFE) {
            return unsafe.getInt(data, BYTE_ARRAY_OFFSET + index);
        }
        else {
            return (data[index] & 0xff) |
                    (data[index + 1] & 0xff) << 8 |
                    (data[index + 2] & 0xff) << 16 |
                    (data[index + 3] & 0xff) << 24;
        }
    }

    static void copyLong(byte[] src, int srcIndex, byte[] dest, int destIndex)
    {
        if (HAS_UNSAFE) {
            long value = unsafe.getLong(src, BYTE_ARRAY_OFFSET + srcIndex);
            unsafe.putLong(dest, BYTE_ARRAY_OFFSET + destIndex, value);
        }
        else {
            // this is only used in the unsafe optimized code
            throw new UnsupportedOperationException();
        }
    }

    static long loadLong(byte[] data, int index)
    {
        if (HAS_UNSAFE) {
            return unsafe.getLong(data, BYTE_ARRAY_OFFSET + index);
        }
        else {
            // this is only used in the unsafe optimized code
            throw new UnsupportedOperationException();
        }
    }

    //
    // Copied from Guava Preconditions
    static void checkArgument(boolean expression, String errorMessageTemplate, Object... errorMessageArgs)
    {
        if (!expression) {
            throw new IllegalArgumentException(
                    String.format(errorMessageTemplate, errorMessageArgs));
        }
    }

    static void checkPositionIndexes(int start, int end, int size)
    {
        // Carefully optimized for execution by hotspot (explanatory comment above)
        if (start < 0 || end < start || end > size) {
            throw new IndexOutOfBoundsException(badPositionIndexes(start, end, size));
        }
    }

    static String badPositionIndexes(int start, int end, int size)
    {
        if (start < 0 || start > size) {
            return badPositionIndex(start, size, "start index");
        }
        if (end < 0 || end > size) {
            return badPositionIndex(end, size, "end index");
        }
        // end < start
        return String.format("end index (%s) must not be less than start index (%s)", end, start);
    }

    static String badPositionIndex(int index, int size, String desc)
    {
        if (index < 0) {
            return String.format("%s (%s) must not be negative", desc, index);
        }
        else if (size < 0) {
            throw new IllegalArgumentException("negative size: " + size);
        }
        else { // index > size
            return String.format("%s (%s) must not be greater than size (%s)", desc, index, size);
        }
    }
}
