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
package io.trino.operator.scalar;

import io.trino.spi.TrinoException;
import io.trino.spi.block.LazyResultValueBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;

import java.util.Optional;

import static io.trino.operator.scalar.SaveEvaluateFunction.NAME;
import static io.trino.spi.block.LazyResultValueBuilder.buildLazyResultWithError;
import static io.trino.spi.block.LazyResultValueBuilder.buildLazyResultWithValue;

@Description("Internal save_evaluate function")
@ScalarFunction(value = NAME, hidden = true, deterministic = false)
public final class SaveEvaluateFunction
{
    public static final String NAME = "$save_evaluate";

    @TypeParameter("T")
    @SqlType("lazyResult(T)")
    public static Object tryObject(@TypeParameter("T") Type type, @SqlType("function(T)") TryObjectLambda function)
    {
        Optional<Object> value = Optional.empty();
        Optional<TrinoException> exception = Optional.empty();
        try {
            value = Optional.ofNullable(function.apply());
        }
        catch (TrinoException e) {
            exception = Optional.of(e);
        }

        if (value.isPresent()) {
            return buildLazyResultWithValue(type, value.get());
        } else {
            return buildLazyResultWithError(type, exception.get());
        }
    }

    @FunctionalInterface
    public interface TryObjectLambda
            extends LambdaFunctionInterface
    {
        Object apply();
    }
}
