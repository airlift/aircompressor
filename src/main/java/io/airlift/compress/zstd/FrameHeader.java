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

import java.util.Objects;
import java.util.StringJoiner;

import static io.airlift.compress.zstd.Util.checkState;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

class FrameHeader
{
    final long headerSize;
    final int windowSize;
    final long contentSize;
    final long dictionaryId;
    final boolean hasChecksum;

    public FrameHeader(long headerSize, int windowSize, long contentSize, long dictionaryId, boolean hasChecksum)
    {
        checkState(windowSize >= 0 || contentSize >= 0, "Invalid frame header: contentSize or windowSize must be set");
        this.headerSize = headerSize;
        this.windowSize = windowSize;
        this.contentSize = contentSize;
        this.dictionaryId = dictionaryId;
        this.hasChecksum = hasChecksum;
    }

    public int computeRequiredOutputBufferLookBackSize()
    {
        if (contentSize < 0) {
            return windowSize;
        }
        if (windowSize < 0) {
            return toIntExact(contentSize);
        }
        return toIntExact(min(windowSize, contentSize));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FrameHeader that = (FrameHeader) o;
        return headerSize == that.headerSize &&
                windowSize == that.windowSize &&
                contentSize == that.contentSize &&
                dictionaryId == that.dictionaryId &&
                hasChecksum == that.hasChecksum;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(headerSize, windowSize, contentSize, dictionaryId, hasChecksum);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", FrameHeader.class.getSimpleName() + "[", "]")
                .add("headerSize=" + headerSize)
                .add("windowSize=" + windowSize)
                .add("contentSize=" + contentSize)
                .add("dictionaryId=" + dictionaryId)
                .add("hasChecksum=" + hasChecksum)
                .toString();
    }
}
