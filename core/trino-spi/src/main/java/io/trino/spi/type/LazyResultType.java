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
package io.trino.spi.type;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.LazyResult;
import io.trino.spi.block.LazyResultBlock;
import io.trino.spi.block.LazyResultBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorMethodHandle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.spi.block.LazyResultValueBuilder.buildLazyResultWithValue;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.StandardTypes.LAZY_RESULT;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;


public class LazyResultType
        extends AbstractType
{
    public static final Type ERROR_MSG_TYPE = CharType.createCharType(128);
    public static final Type ERROR_CODE_TYPE = IntegerType.INTEGER;

    private static final InvocationConvention READ_FLAT_CONVENTION = simpleConvention(FAIL_ON_NULL, FLAT);
    private static final InvocationConvention READ_FLAT_TO_BLOCK_CONVENTION = simpleConvention(BLOCK_BUILDER, FLAT);
    private static final InvocationConvention WRITE_FLAT_CONVENTION = simpleConvention(FLAT_RETURN, NEVER_NULL);
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention IDENTICAL_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE);
    private static final InvocationConvention COMPARISON_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL);

    private static final MethodHandle READ_FLAT;
    private static final MethodHandle READ_FLAT_TO_BLOCK;
    private static final MethodHandle WRITE_FLAT;
    private static final MethodHandle CHAIN_EQUAL;
    private static final MethodHandle CHAIN_HASH_CODE;
    private static final MethodHandle CHAIN_IDENTICAL_START;
    private static final MethodHandle CHAIN_IDENTICAL;
    private static final MethodHandle CHAIN_INDETERMINATE;
    private static final MethodHandle CHAIN_COMPARISON;

    // this field is used in double-checked locking
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile TypeOperatorDeclaration typeOperatorDeclaration;

    static {
        try {
            Lookup lookup = lookup();
            READ_FLAT = lookup.findStatic(LazyResultType.class, "megamorphicReadFlat", methodType(LazyResult.class, LazyResultType.class, List.class, byte[].class, int.class, byte[].class));
            READ_FLAT_TO_BLOCK = lookup.findStatic(LazyResultType.class, "megamorphicReadFlatToBlock", methodType(void.class, LazyResultType.class, List.class, byte[].class, int.class, byte[].class, BlockBuilder.class));
            WRITE_FLAT = lookup.findStatic(LazyResultType.class, "megamorphicWriteFlat", methodType(void.class, LazyResultType.class, List.class, LazyResult.class, byte[].class, int.class, byte[].class, int.class));
            CHAIN_EQUAL = lookup.findStatic(LazyResultType.class, "chainEqual", methodType(Boolean.class, Boolean.class, int.class, MethodHandle.class, LazyResult.class, LazyResult.class));
            CHAIN_HASH_CODE = lookup.findStatic(LazyResultType.class, "chainHashCode", methodType(long.class, long.class, int.class, MethodHandle.class, LazyResult.class));
            CHAIN_IDENTICAL_START = lookup.findStatic(LazyResultType.class, "chainIdenticalStart", methodType(boolean.class, MethodHandle.class, LazyResult.class, LazyResult.class));
            CHAIN_IDENTICAL = lookup.findStatic(LazyResultType.class, "chainIdentical", methodType(boolean.class, boolean.class, int.class, MethodHandle.class, LazyResult.class, LazyResult.class));
            CHAIN_INDETERMINATE = lookup.findStatic(LazyResultType.class, "chainIndeterminate", methodType(boolean.class, boolean.class, int.class, MethodHandle.class, LazyResult.class));
            CHAIN_COMPARISON = lookup.findStatic(LazyResultType.class, "chainComparison", methodType(long.class, long.class, int.class, MethodHandle.class, LazyResult.class, LazyResult.class));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final Type type;
    private final List<Type> fields;
    private final boolean comparable;
    private final boolean orderable;
    private final int flatFixedSize;
    private final boolean flatVariableWidth;

    public LazyResultType(Type elementType)
    {
        super(new TypeSignature(LAZY_RESULT, TypeSignatureParameter.typeParameter(elementType.getTypeSignature())), LazyResult.class, LazyResultBlock.class);

        this.type = elementType;
        this.fields = List.of(elementType, ERROR_CODE_TYPE, ERROR_MSG_TYPE);

        this.comparable = fields.stream().allMatch(Type::isComparable);
        this.orderable = fields.stream().allMatch(Type::isOrderable);

        // flat fixed size is one null byte for each field plus the sum of the field fixed sizes
        int fixedSize = fields.size();
        for (Type fieldType : fields) {
            fixedSize += fieldType.getFlatFixedSize();
        }
        flatFixedSize = fixedSize;
        this.flatVariableWidth = fields.stream().anyMatch(Type::isFlatVariableWidth);
    }

    @Override
    public LazyResultBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new LazyResultBlockBuilder(fields, blockBuilderStatus, expectedEntries);
    }

    @Override
    public LazyResultBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new LazyResultBlockBuilder(fields, blockBuilderStatus, expectedEntries);
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public String getDisplayName()
    {
        return LAZY_RESULT + "(" + type + ")";
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        LazyResult lazyResult = getObject(block, position);
        List<Object> values = new ArrayList<>(lazyResult.getFieldCount());

        int rawIndex = lazyResult.getRawIndex();
        for (int i = 0; i < lazyResult.getFieldCount(); i++) {
            values.add(fields.get(i).getObjectValue(session, lazyResult.getRawFieldBlock(i), rawIndex));
        }

        return Collections.unmodifiableList(values);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        } else {
            writeObject(blockBuilder, getObject(block, position));
        }
    }

    @Override
    public LazyResult getObject(Block block, int position)
    {
        return read((LazyResultBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        LazyResult lazyResult = (LazyResult) value;
        int rawIndex = lazyResult.getRawIndex();
        ((LazyResultBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> {
            for (int i = 0; i < lazyResult.getFieldCount(); i++) {
                fields.get(i).appendTo(lazyResult.getRawFieldBlock(i), rawIndex, fieldBuilders.get(i));
            }
        });
    }

    @Override
    public int getFlatFixedSize()
    {
        return flatFixedSize;
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        return flatVariableWidth;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        if (!flatVariableWidth) {
            return 0;
        }

        LazyResult lazyResult = getObject(block, position);
        int rawIndex = lazyResult.getRawIndex();

        int variableSize = 0;
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = fields.get(i);
            Block fieldBlock = lazyResult.getRawFieldBlock(i);
            if (!fieldBlock.isNull(rawIndex)) {
                variableSize += fieldType.getFlatVariableWidthSize(fieldBlock, rawIndex);
            }
        }
        return variableSize;
    }

    @Override
    public int relocateFlatVariableWidthOffsets(byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        if (!flatVariableWidth) {
            return 0;
        }

        int totalVariableSize = 0;
        for (Type fieldType : fields) {
            if (fieldType.isFlatVariableWidth() && fixedSizeSlice[fixedSizeOffset] == 0) {
                totalVariableSize += fieldType.relocateFlatVariableWidthOffsets(fixedSizeSlice, fixedSizeOffset + 1, variableSizeSlice, variableSizeOffset + totalVariableSize);
            }
            fixedSizeOffset += 1 + fieldType.getFlatFixedSize();
        }
        return totalVariableSize;
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return List.of(type);
    }

    public List<Type> getFieldTypes()
    {
        return fields;
    }

    @Override
    public boolean isComparable()
    {
        return comparable;
    }

    @Override
    public boolean isOrderable()
    {
        return orderable;
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        if (typeOperatorDeclaration == null) {
            generateTypeOperators(typeOperators);
        }
        return typeOperatorDeclaration;
    }

    private void generateTypeOperators(TypeOperators typeOperators)
    {
        if (typeOperatorDeclaration != null) {
            return;
        }
        typeOperatorDeclaration = TypeOperatorDeclaration.builder(getJavaType())
                .addReadValueOperators(getReadValueOperatorMethodHandles(typeOperators))
                .addEqualOperators(getEqualOperatorMethodHandles(typeOperators, fields))
                .addHashCodeOperators(getHashCodeOperatorMethodHandles(typeOperators, fields))
                .addXxHash64Operators(getXxHash64OperatorMethodHandles(typeOperators, fields))
                .addIdenticalOperators(getIdenticalOperatorInvokers(typeOperators, fields))
                .addIndeterminateOperators(getIndeterminateOperatorInvokers(typeOperators, fields))
                .addComparisonUnorderedLastOperators(getComparisonOperatorInvokers(typeOperators::getComparisonUnorderedLastOperator, fields))
                .addComparisonUnorderedFirstOperators(getComparisonOperatorInvokers(typeOperators::getComparisonUnorderedFirstOperator, fields))
                .build();
    }

    private List<OperatorMethodHandle> getReadValueOperatorMethodHandles(TypeOperators typeOperators)
    {
        List<MethodHandle> fieldReadFlatMethods = fields.stream()
                .map(type -> typeOperators.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT)))
                .toList();
        MethodHandle readFlat = insertArguments(READ_FLAT, 0, this, fieldReadFlatMethods);
        MethodHandle readFlatToBlock = insertArguments(READ_FLAT_TO_BLOCK, 0, this, fieldReadFlatMethods);

        List<MethodHandle> fieldWriteFlatMethods = fields.stream()
                .map(type -> typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION)))
                .toList();
        MethodHandle writeFlat = insertArguments(WRITE_FLAT, 0, this, fieldWriteFlatMethods);

        return List.of(
                new OperatorMethodHandle(READ_FLAT_CONVENTION, readFlat),
                new OperatorMethodHandle(READ_FLAT_TO_BLOCK_CONVENTION, readFlatToBlock),
                new OperatorMethodHandle(WRITE_FLAT_CONVENTION, writeFlat));
    }

    private static LazyResult read(LazyResultBlock block, int position)
    {
        return block.getRow(position);
    }

    private static LazyResult megamorphicReadFlat(
            LazyResultType rowType,
            List<MethodHandle> fieldReadFlatMethods,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice)
    {
        return buildLazyResultWithValue(rowType, fieldBuilders ->
        {
            try {
                readFlatFields(rowType, fieldReadFlatMethods, fixedSizeSlice, fixedSizeOffset, variableSizeSlice, fieldBuilders);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void megamorphicReadFlatToBlock(
            LazyResultType rowType,
            List<MethodHandle> fieldReadFlatMethods,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            BlockBuilder blockBuilder)
            throws Throwable
    {
        ((LazyResultBlockBuilder) blockBuilder).buildEntry(fieldBuilders ->
                readFlatFields(rowType, fieldReadFlatMethods, fixedSizeSlice, fixedSizeOffset, variableSizeSlice, fieldBuilders));
    }

    private static void readFlatFields(
            LazyResultType rowType,
            List<MethodHandle> fieldReadFlatMethods,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            List<BlockBuilder> fieldBuilders)
            throws Throwable
    {
        List<Type> fieldTypes = rowType.fields;
        for (int fieldIndex = 0; fieldIndex < fieldTypes.size(); fieldIndex++) {
            Type fieldType = fieldTypes.get(fieldIndex);
            BlockBuilder fieldBuilder = fieldBuilders.get(fieldIndex);

            boolean isNull = fixedSizeSlice[fixedSizeOffset] != 0;
            if (isNull) {
                fieldBuilder.appendNull();
            } else {
                fieldReadFlatMethods.get(fieldIndex).invokeExact(fixedSizeSlice, fixedSizeOffset + 1, variableSizeSlice, fieldBuilder);
            }
            fixedSizeOffset += 1 + fieldType.getFlatFixedSize();
        }
    }

    private static void megamorphicWriteFlat(
            LazyResultType rowType,
            List<MethodHandle> fieldWriteFlatMethods,
            LazyResult row,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
            throws Throwable
    {
        int rawIndex = row.getRawIndex();
        List<Type> fieldTypes = rowType.fields;
        for (int fieldIndex = 0; fieldIndex < fieldTypes.size(); fieldIndex++) {
            Type fieldType = fieldTypes.get(fieldIndex);
            Block fieldBlock = row.getRawFieldBlock(fieldIndex);
            if (fieldBlock.isNull(rawIndex)) {
                fixedSizeSlice[fixedSizeOffset] = 1;
            } else {
                int fieldVariableLength = 0;
                if (fieldType.isFlatVariableWidth()) {
                    fieldVariableLength = fieldType.getFlatVariableWidthSize(fieldBlock, rawIndex);
                }
                fieldWriteFlatMethods.get(fieldIndex).invokeExact((Block) fieldBlock, rawIndex, fixedSizeSlice, fixedSizeOffset + 1, variableSizeSlice, variableSizeOffset);
                variableSizeOffset += fieldVariableLength;
            }
            fixedSizeOffset += 1 + fieldType.getFlatFixedSize();
        }
    }

    private static List<OperatorMethodHandle> getEqualOperatorMethodHandles(TypeOperators typeOperators, List<Type> fields)
    {
        boolean comparable = fields.stream().allMatch(Type::isComparable);
        if (!comparable) {
            return emptyList();
        }

        // (LazyResult, LazyResult):Boolean
        MethodHandle equal = dropArguments(constant(Boolean.class, TRUE), 0, LazyResult.class, LazyResult.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Type field = fields.get(fieldId);
            // (LazyResult, LazyResult, int, MethodHandle, LazyResult, LazyResult):Boolean
            equal = collectArguments(
                    CHAIN_EQUAL,
                    0,
                    equal);

            // field equal
            MethodHandle fieldEqualOperator = typeOperators.getEqualOperator(field, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));

            // (LazyResult, LazyResult, LazyResult, LazyResult):Boolean
            equal = insertArguments(equal, 2, fieldId, fieldEqualOperator);

            // (LazyResult, LazyResult):Boolean
            equal = permuteArguments(equal, methodType(Boolean.class, LazyResult.class, LazyResult.class), 0, 1, 0, 1);
        }
        return singletonList(new OperatorMethodHandle(EQUAL_CONVENTION, equal));
    }

    private static Boolean chainEqual(Boolean previousFieldsEqual, int currentFieldIndex, MethodHandle currentFieldEqual, LazyResult leftRow, LazyResult rightRow)
            throws Throwable
    {
        if (previousFieldsEqual == FALSE) {
            return FALSE;
        }

        int leftRawIndex = leftRow.getRawIndex();
        int rightRawIndex = rightRow.getRawIndex();
        Block leftFieldBlock = leftRow.getRawFieldBlock(currentFieldIndex);
        Block rightFieldBlock = rightRow.getRawFieldBlock(currentFieldIndex);

        if (leftFieldBlock.isNull(leftRawIndex) || rightFieldBlock.isNull(rightRawIndex)) {
            return null;
        }

        Boolean result = (Boolean) currentFieldEqual.invokeExact(leftFieldBlock, leftRawIndex, rightFieldBlock, rightRawIndex);
        if (result == TRUE) {
            // this field is equal, so the result is either true or unknown depending on the previous fields
            return previousFieldsEqual;
        }
        // this field is either not equal or unknown, which is the result
        return result;
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(TypeOperators typeOperators, List<Type> fields)
    {
        return getHashCodeOperatorMethodHandles(fields, type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
    }

    private static List<OperatorMethodHandle> getXxHash64OperatorMethodHandles(TypeOperators typeOperators, List<Type> fields)
    {
        return getHashCodeOperatorMethodHandles(fields, type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(List<Type> fields, Function<Type, MethodHandle> getHashOperator)
    {
        boolean comparable = fields.stream().allMatch(Type::isComparable);
        if (!comparable) {
            return emptyList();
        }

        // (LazyResult):long
        MethodHandle hashCode = dropArguments(constant(long.class, 1), 0, LazyResult.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Type field = fields.get(fieldId);
            // (LazyResult, int, MethodHandle, LazyResult):long
            hashCode = collectArguments(
                    CHAIN_HASH_CODE,
                    0,
                    hashCode);

            // field hash code
            MethodHandle fieldHashCodeOperator = getHashOperator.apply(field);

            // (LazyResult, LazyResult):long
            hashCode = insertArguments(hashCode, 1, fieldId, fieldHashCodeOperator);

            // (LazyResult):long
            hashCode = permuteArguments(hashCode, methodType(long.class, LazyResult.class), 0, 0);
        }
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, hashCode));
    }

    private static long chainHashCode(long previousFieldHashCode, int currentFieldIndex, MethodHandle currentFieldHashCodeOperator, LazyResult row)
            throws Throwable
    {
        Block fieldBlock = row.getRawFieldBlock(currentFieldIndex);
        int rawIndex = row.getRawIndex();

        long fieldHashCode = NULL_HASH_CODE;
        if (!fieldBlock.isNull(rawIndex)) {
            fieldHashCode = (long) currentFieldHashCodeOperator.invokeExact(fieldBlock, rawIndex);
        }
        return 31 * previousFieldHashCode + fieldHashCode;
    }

    private static List<OperatorMethodHandle> getIdenticalOperatorInvokers(TypeOperators typeOperators, List<Type> fields)
    {
        boolean comparable = fields.stream().allMatch(Type::isComparable);
        if (!comparable) {
            return emptyList();
        }
        // (LazyResult, LazyResult):boolean
        MethodHandle identical = dropArguments(constant(boolean.class, true), 0, LazyResult.class, LazyResult.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Type field = fields.get(fieldId);
            // (LazyResult, LazyResult, int, MethodHandle, LazyResult, LazyResult):boolean
            identical = collectArguments(
                    CHAIN_IDENTICAL,
                    0,
                    identical);

            // field identical
            MethodHandle fieldIdenticalOperator = typeOperators.getIdenticalOperator(field, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));

            // (LazyResult, LazyResult, LazyResult, LazyResult):boolean
            identical = insertArguments(identical, 2, fieldId, fieldIdenticalOperator);

            // (LazyResult, LazyResult):boolean
            identical = permuteArguments(identical, methodType(boolean.class, LazyResult.class, LazyResult.class), 0, 1, 0, 1);
        }
        identical = CHAIN_IDENTICAL_START.bindTo(identical);

        return singletonList(new OperatorMethodHandle(IDENTICAL_CONVENTION, identical));
    }

    private static boolean chainIdenticalStart(MethodHandle chain, LazyResult leftRow, LazyResult rightRow)
            throws Throwable
    {
        boolean leftIsNull = leftRow == null;
        boolean rightIsNull = rightRow == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull == rightIsNull;
        }
        return (boolean) chain.invokeExact(leftRow, rightRow);
    }

    private static boolean chainIdentical(boolean previousFieldsIdentical, int currentFieldIndex, MethodHandle currentFieldIdentical, LazyResult leftRow, LazyResult rightRow)
            throws Throwable
    {
        if (!previousFieldsIdentical) {
            return false;
        }
        return (boolean) currentFieldIdentical.invokeExact(
                leftRow.getRawFieldBlock(currentFieldIndex), leftRow.getRawIndex(),
                rightRow.getRawFieldBlock(currentFieldIndex), rightRow.getRawIndex());
    }

    private static List<OperatorMethodHandle> getIndeterminateOperatorInvokers(TypeOperators typeOperators, List<Type> fields)
    {
        boolean comparable = fields.stream().allMatch(Type::isComparable);
        if (!comparable) {
            return emptyList();
        }

        // (LazyResult):long
        MethodHandle indeterminate = dropArguments(constant(boolean.class, false), 0, LazyResult.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Type field = fields.get(fieldId);
            // (LazyResult, int, MethodHandle, LazyResult):boolean
            indeterminate = collectArguments(
                    CHAIN_INDETERMINATE,
                    0,
                    indeterminate);

            // field indeterminate
            MethodHandle fieldIndeterminateOperator = typeOperators.getIndeterminateOperator(field, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));

            // (LazyResult, LazyResult):boolean
            indeterminate = insertArguments(indeterminate, 1, fieldId, fieldIndeterminateOperator);

            // (LazyResult):boolean
            indeterminate = permuteArguments(indeterminate, methodType(boolean.class, LazyResult.class), 0, 0);
        }
        return singletonList(new OperatorMethodHandle(INDETERMINATE_CONVENTION, indeterminate));
    }

    private static boolean chainIndeterminate(boolean previousFieldIndeterminate, int currentFieldIndex, MethodHandle currentFieldIndeterminateOperator, LazyResult row)
            throws Throwable
    {
        if (row == null || previousFieldIndeterminate) {
            return true;
        }
        int rawIndex = row.getRawIndex();
        Block fieldBlock = row.getRawFieldBlock(currentFieldIndex);
        if (fieldBlock.isNull(rawIndex)) {
            return true;
        }
        return (boolean) currentFieldIndeterminateOperator.invokeExact(fieldBlock, rawIndex);
    }

    private static List<OperatorMethodHandle> getComparisonOperatorInvokers(BiFunction<Type, InvocationConvention, MethodHandle> comparisonOperatorFactory, List<Type> fields)
    {
        boolean orderable = fields.stream().allMatch(Type::isOrderable);
        if (!orderable) {
            return emptyList();
        }

        // (LazyResult, LazyResult):Boolean
        MethodHandle comparison = dropArguments(constant(long.class, 0), 0, LazyResult.class, LazyResult.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Type field = fields.get(fieldId);
            // (LazyResult, LazyResult, int, MethodHandle, LazyResult, LazyResult):Boolean
            comparison = collectArguments(
                    CHAIN_COMPARISON,
                    0,
                    comparison);

            // field comparison
            MethodHandle fieldComparisonOperator = comparisonOperatorFactory.apply(field, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));

            // (LazyResult, LazyResult, LazyResult, LazyResult):Boolean
            comparison = insertArguments(comparison, 2, fieldId, fieldComparisonOperator);

            // (LazyResult, LazyResult):Boolean
            comparison = permuteArguments(comparison, methodType(long.class, LazyResult.class, LazyResult.class), 0, 1, 0, 1);
        }
        return singletonList(new OperatorMethodHandle(COMPARISON_CONVENTION, comparison));
    }

    private static long chainComparison(long previousFieldsResult, int fieldIndex, MethodHandle nextFieldComparison, LazyResult leftRow, LazyResult rightRow)
            throws Throwable
    {
        if (previousFieldsResult != 0) {
            return previousFieldsResult;
        }

        int leftRawIndex = leftRow.getRawIndex();
        int rightRawIndex = rightRow.getRawIndex();
        Block leftFieldBlock = leftRow.getRawFieldBlock(fieldIndex);
        Block rightFieldBlock = rightRow.getRawFieldBlock(fieldIndex);

        checkElementNotNull(leftFieldBlock.isNull(leftRawIndex));
        checkElementNotNull(rightFieldBlock.isNull(rightRawIndex));

        return (long) nextFieldComparison.invokeExact(leftFieldBlock, leftRawIndex, rightFieldBlock, rightRawIndex);
    }

    private static void checkElementNotNull(boolean isNull)
    {
        if (isNull) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for fields with null elements");
        }
    }
}
