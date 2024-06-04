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
package io.trino.spi.block;

import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LazyResultType;
import io.trino.spi.type.Type;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.LazyResultType.ERROR_CODE_TYPE;
import static io.trino.spi.type.LazyResultType.ERROR_MSG_TYPE;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.min;

public interface LazyResultValueBuilder<E extends Throwable>
{
    static <E extends Throwable> LazyResult buildLazyResultWithValue(LazyResultType rowType, LazyResultValueBuilder<E> builder)
            throws E
    {
        return new BufferedLazyResultValueBuilder(rowType, 1)
                .build(builder);
    }

    static LazyResult buildLazyResultWithValue(Type type, Object o)
    {
        return new BufferedLazyResultValueBuilder(new LazyResultType(type), 1)
                .build(builder -> {
                    writeNativeValue(type, builder.getFirst(), o);
                    builder.get(1).appendNull();
                    builder.get(2).appendNull();
                });
    }

    static LazyResult buildLazyResultWithError(Type type, TrinoException exception)
    {
        return new BufferedLazyResultValueBuilder(new LazyResultType(type), 1)
                .build(builder -> {
                    builder.getFirst().appendNull();
                    int length = min(exception.getMessage().length(), ((CharType) ERROR_MSG_TYPE).getLength());
                    ERROR_CODE_TYPE.writeLong(builder.get(1), exception.getErrorCode().getCode());
                    ERROR_MSG_TYPE.writeSlice(builder.get(2), utf8Slice(exception.getMessage().substring(0, length)));
                });
    }

    void build(List<BlockBuilder> fieldBuilders)
            throws E;
}
