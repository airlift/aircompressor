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

import com.google.common.collect.ImmutableList;
import io.airlift.compress.benchmark.DataSet;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.util.List;

public final class TestingData
{
    public static final List<DataSet> DATA_SETS;

    static {
        try {
            String[] testNames = DataSet.class
                    .getDeclaredField("name")
                    .getAnnotation(Param.class)
                    .value();

            ImmutableList.Builder<DataSet> result = ImmutableList.builder();
            for (String testName : testNames) {
                DataSet entry = new DataSet(testName);
                entry.loadFile();
                result.add(entry);
            }

            DATA_SETS = result.build();
        }
        catch (NoSuchFieldException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private TestingData() {}
}
