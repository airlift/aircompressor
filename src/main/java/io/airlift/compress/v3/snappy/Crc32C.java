/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.v3.snappy;

import java.util.zip.CRC32C;

/**
 * CRC32-C checksum (Castagnoli polynomial, the same used by iSCSI and SSE4.2).
 * <p>
 * Delegates to {@link java.util.zip.CRC32C}, which is a JVM intrinsic backed by
 * the hardware CRC32C instruction (SSE4.2 on x86, CRC on AArch64), so it is
 * several times faster than a table-based implementation.
 */
final class Crc32C
{
    private static final int MASK_DELTA = 0xa282ead8;

    private Crc32C() {}

    public static int maskedCrc32c(byte[] data)
    {
        return maskedCrc32c(data, 0, data.length);
    }

    public static int maskedCrc32c(byte[] data, int offset, int length)
    {
        CRC32C crc32c = new CRC32C();
        crc32c.update(data, offset, length);
        return mask((int) crc32c.getValue());
    }

    /**
     * Return a masked representation of crc.
     * <p/>
     * Motivation: it is problematic to compute the CRC of a string that
     * contains embedded CRCs.  Therefore we recommend that CRCs stored
     * somewhere (e.g., in files) should be masked before being stored.
     */
    public static int mask(int crc)
    {
        // Rotate right by 15 bits and add a constant.
        return ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
    }

    /**
     * Return the crc whose masked representation is masked_crc.
     */
    public static int unmask(int maskedCrc)
    {
        int rot = maskedCrc - MASK_DELTA;
        return ((rot >>> 17) | (rot << 15));
    }
}
