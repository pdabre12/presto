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

package com.facebook.presto.parquet.batchreader.dictionary;

import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.batchreader.BytesUtils;
import com.facebook.presto.parquet.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class BinaryBatchDictionary
        extends Dictionary
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BinaryBatchDictionary.class).instanceSize();

    private final byte[] pageBuffer;
    private final int dictionarySize;
    private final int[] offsets;
    private final Integer length;

    public BinaryBatchDictionary(DictionaryPage dictionaryPage)
    {
        this(dictionaryPage, null);
    }

    public BinaryBatchDictionary(DictionaryPage dictionaryPage, Integer length)
    {
        super(dictionaryPage.getEncoding());
        requireNonNull(dictionaryPage, "dictionaryPage is null");
        checkArgument(dictionaryPage.getDictionarySize() >= 0, "Dictionary size should be greater than or equal to zero");

        this.dictionarySize = dictionaryPage.getDictionarySize();
        this.pageBuffer = requireNonNull(dictionaryPage.getSlice(), "dictionary slice is null").getBytes();
        this.length = length;

        // initialize the offsets array
        IntList offsetList = new IntArrayList();
        int offset = 0;
        if (length == null) {
            while (offset < pageBuffer.length) {
                int len = BytesUtils.getInt(pageBuffer, offset);
                offsetList.add(offset);
                offset += (4 + len);
            }
            offsetList.add(offset);
        }
        else {
            int index = 0;
            while (index < dictionarySize) {
                offsetList.add(offset);
                offset += length;
                index++;
            }
            offsetList.add(offset);
        }

        this.offsets = offsetList.toIntArray();

        checkArgument(offsets.length - 1 == dictionarySize, "Dictionary size and number of entries don't match");
    }

    public int getLength(int dictionaryId)
    {
        checkArgument(dictionaryId >= 0 && dictionaryId < dictionarySize, "invalid dictionary id: %s", dictionaryId);
        if (length == null) {
            return offsets[dictionaryId + 1] - (offsets[dictionaryId] + 4);
        }
        else {
            return length;
        }
    }

    public int copyTo(byte[] byteBuffer, int offset, int dictionaryId)
    {
        if (length == null) {
            int len = offsets[dictionaryId + 1] - (offsets[dictionaryId] + 4);
            System.arraycopy(pageBuffer, offsets[dictionaryId] + 4, byteBuffer, offset, len);
            return len;
        }
        else {
            System.arraycopy(pageBuffer, offsets[dictionaryId], byteBuffer, offset, length);
            return length;
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(pageBuffer) + sizeOf(offsets);
    }
}
