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
package io.airlift.compress.zstd;

import java.util.Arrays;

class BlockCompressionState
{
    public final int[] hashTable;
    public final int[] chainTable;

    private final long baseAddress;

    // starting point of the window with respect to baseAddress
    private int windowBaseOffset;

    public BlockCompressionState(CompressionParameters parameters, long baseAddress)
    {
        this.baseAddress = baseAddress;
        hashTable = new int[1 << parameters.getHashLog()];
        chainTable = new int[1 << parameters.getChainLog()]; // TODO: chain table not used by Strategy.FAST
    }

    public void slideWindow(int slideWindowSize)
    {
        for (int i = 0; i < hashTable.length; i++) {
            int newValue = hashTable[i] - slideWindowSize;
            // if new value is negative, set it to zero branchless
            newValue = newValue & (~(newValue >> 31));
            hashTable[i] = newValue;
        }
        for (int i = 0; i < chainTable.length; i++) {
            int newValue = chainTable[i] - slideWindowSize;
            // if new value is negative, set it to zero branchless
            newValue = newValue & (~(newValue >> 31));
            chainTable[i] = newValue;
        }
    }

    public void reset()
    {
        Arrays.fill(hashTable, 0);
        Arrays.fill(chainTable, 0);
    }

    public void enforceMaxDistance(long inputLimit, int maxDistance)
    {
        int distance = (int) (inputLimit - baseAddress);

        int newOffset = distance - maxDistance;
        if (windowBaseOffset < newOffset) {
            windowBaseOffset = newOffset;
        }
    }

    public long getBaseAddress()
    {
        return baseAddress;
    }

    public int getWindowBaseOffset()
    {
        return windowBaseOffset;
    }
}
