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
package io.airlift.compress.v3;

public class MalformedInputException
        extends RuntimeException
{
    private final long offset;

    public MalformedInputException(long offset)
    {
        this(offset, "Malformed input");
    }

    public MalformedInputException(long offset, String reason)
    {
        super(reason + ": offset=" + offset);
        this.offset = offset;
    }

    public long getOffset()
    {
        return offset;
    }
}
