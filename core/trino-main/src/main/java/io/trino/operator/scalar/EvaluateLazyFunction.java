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
import io.trino.spi.type.LazyResultType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.spi.type.TypeSignature.lazyResultType;
import static io.trino.util.Reflection.methodHandle;

/**
 * This scalar function exists primarily to test lambda expression support.
 */
public final class EvaluateLazyFunction
        extends SqlScalarFunction
{
    public static final String NAME = "$eval_func";
    public static final EvaluateLazyFunction EVALUATE_FUNCTION = new EvaluateLazyFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(EvaluateLazyFunction.class, "evaluate", EvaluateInvokeLambda.class);

    private EvaluateLazyFunction()
    {
        super(FunctionMetadata.scalarBuilder(NAME)
                .signature(Signature.builder()
                        .typeVariable("T")
                        .returnType(new TypeSignature("T"))
                        .argumentType(functionType(lazyResultType(new TypeSignature("T"))))
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
                ImmutableList.of(FUNCTION),
                ImmutableList.of(EvaluateInvokeLambda.class),
                METHOD_HANDLE.asType(
                        METHOD_HANDLE.type()
                                .changeReturnType(returnType.getJavaType())),
                Optional.empty());
    }

    public static Object evaluate(EvaluateInvokeLambda function)
    {
        Object result = function.apply();
        if (!(result instanceof LazyResult lazyResult)) {
            return result;
        }
        Optional<TrinoException> error = lazyResult.getError();
        if (error.isPresent()) {
            throw error.get();
        }
        return lazyResult.getValue();
    }

    @FunctionalInterface
    public interface EvaluateInvokeLambda
            extends LambdaFunctionInterface
    {
        Object apply();
    }
}
