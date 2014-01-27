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
package io.airlift.compress;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@AuxCounters
@State(Scope.Thread)
public class Counters
{
    private long compressedBytes;
    private long uncompressedBytes;

    @Setup(Level.Iteration)
    public void reset()
    {
        compressedBytes = 0;
        uncompressedBytes = 0;
    }

    public void recordCompressed(long bytes)
    {
        compressedBytes += bytes;
    }

    public void recordUncompressed(long bytes)
    {
        uncompressedBytes += bytes;
    }

    public long getCompressedBytes()
    {
        return compressedBytes;
    }

    public long getUncompressedBytes()
    {
        return uncompressedBytes;
    }

}
