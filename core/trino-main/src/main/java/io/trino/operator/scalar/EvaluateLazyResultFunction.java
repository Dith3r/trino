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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.LazyResult;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeSignature.lazyResultType;
import static io.trino.util.Reflection.methodHandle;

/**
 * This scalar function exists primarily to test lambda expression support.
 */
public final class EvaluateLazyResultFunction
        extends SqlScalarFunction
{
    public static final String NAME = "$eval_lazy_result";
    public static final EvaluateLazyResultFunction EVALUATE_FUNCTION = new EvaluateLazyResultFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(EvaluateLazyResultFunction.class, "evaluate", Object.class);

    private EvaluateLazyResultFunction()
    {
        super(FunctionMetadata.scalarBuilder(NAME)
                .signature(Signature.builder()
                        .typeVariable("T")
                        .returnType(new TypeSignature("T"))
                        .argumentType(lazyResultType(new TypeSignature("T")))
                        .build())
                .nullable()
                .hidden()
                .nondeterministic()
                .description("lambda invoke function")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type returnType = boundSignature.getReturnType();
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL),
                ImmutableList.of(),
                METHOD_HANDLE.asType(
                        METHOD_HANDLE.type()
                                .changeReturnType(returnType.getJavaType())),
                Optional.empty());
    }

    public static Object evaluate(Object object)
    {
        if (object instanceof LazyResult lazyResult) {
            Optional<TrinoException> error = lazyResult.getError();
            if (error.isPresent()) {
                throw error.get();
            }
            return lazyResult.getValue();
        }
        return object;
    }

    @FunctionalInterface
    public interface EvaluateInvokeLambda
            extends LambdaFunctionInterface
    {
        Object apply();
    }
}
