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

package com.facebook.presto.builtin.tools;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.LongVariableConstraint;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class WorkerFunctionUtil
{
    private static final Set<String> PARAMETERIZED_TYPES = ImmutableSet.of(VARCHAR);

    private WorkerFunctionUtil() {}

    public static synchronized SqlInvokedFunction createSqlInvokedFunction(String functionName, JsonBasedUdfFunctionMetadata jsonBasedUdfFunctionMetaData, String catalogName)
    {
        checkState(jsonBasedUdfFunctionMetaData.getRoutineCharacteristics().getLanguage().equals(RoutineCharacteristics.Language.CPP), "WorkerFunctionUtil only supports CPP UDF");
        QualifiedObjectName qualifiedFunctionName = QualifiedObjectName.valueOf(new CatalogSchemaName(catalogName, jsonBasedUdfFunctionMetaData.getSchema()), functionName);
        List<String> parameterNameList = jsonBasedUdfFunctionMetaData.getParamNames();
        List<TypeSignature> parameterTypeList = convertApplicableTypeToVariable(jsonBasedUdfFunctionMetaData.getParamTypes());
        List<TypeVariableConstraint> typeVariableConstraintsList = jsonBasedUdfFunctionMetaData.getTypeVariableConstraints().isPresent() ?
                jsonBasedUdfFunctionMetaData.getTypeVariableConstraints().get() : ImmutableList.of();
        List<LongVariableConstraint> longVariableConstraintList = jsonBasedUdfFunctionMetaData.getLongVariableConstraints().isPresent() ?
                jsonBasedUdfFunctionMetaData.getLongVariableConstraints().get() : ImmutableList.of();

        TypeSignature outputType = convertApplicableTypeToVariable(jsonBasedUdfFunctionMetaData.getOutputType());
        ImmutableList.Builder<Parameter> parameterBuilder = ImmutableList.builder();
        for (int i = 0; i < parameterNameList.size(); i++) {
            parameterBuilder.add(new Parameter(parameterNameList.get(i), parameterTypeList.get(i)));
        }

        Optional<AggregationFunctionMetadata> aggregationFunctionMetadata =
                jsonBasedUdfFunctionMetaData.getAggregateMetadata()
                        .map(metadata -> new AggregationFunctionMetadata(
                                convertApplicableTypeToVariable(metadata.getIntermediateType()),
                                metadata.isOrderSensitive()));

        return new SqlInvokedFunction(
                qualifiedFunctionName,
                parameterBuilder.build(),
                typeVariableConstraintsList,
                longVariableConstraintList,
                outputType,
                jsonBasedUdfFunctionMetaData.getDocString(),
                jsonBasedUdfFunctionMetaData.getRoutineCharacteristics(),
                "",
                jsonBasedUdfFunctionMetaData.getVariableArity(),
                notVersioned(),
                jsonBasedUdfFunctionMetaData.getFunctionKind(),
                aggregationFunctionMetadata);
    }

    // Todo: Improve the handling of parameter type differentiation in native execution.
    // HACK: Currently, we lack support for correctly identifying the parameterKind, specifically between TYPE and VARIABLE,
    // in native execution. The following utility functions help bridge this gap by parsing the type signature and verifying whether its base
    // and parameters are of a supported type. The valid types list are non - parametric types that Presto supports.
    public static List<TypeSignature> convertApplicableTypeToVariable(List<TypeSignature> typeSignatures)
    {
        List<TypeSignature> newTypeSignaturesList = new ArrayList<>();
        for (TypeSignature typeSignature : typeSignatures) {
            if (!typeSignature.getParameters().isEmpty()) {
                TypeSignature newTypeSignature =
                        new TypeSignature(
                                typeSignature.getBase(),
                                getTypeSignatureParameters(
                                        typeSignature,
                                        typeSignature.getParameters()));
                newTypeSignaturesList.add(newTypeSignature);
            }
            else {
                newTypeSignaturesList.add(typeSignature);
            }
        }
        return newTypeSignaturesList;
    }

    public static TypeSignature convertApplicableTypeToVariable(TypeSignature typeSignature)
    {
        List<TypeSignature> typeSignaturesList = convertApplicableTypeToVariable(ImmutableList.of(typeSignature));
        checkArgument(!typeSignaturesList.isEmpty(), "Type signature list is empty for : " + typeSignature);
        return typeSignaturesList.get(0);
    }

    private static List<TypeSignatureParameter> getTypeSignatureParameters(
            TypeSignature typeSignature,
            List<TypeSignatureParameter> typeSignatureParameterList)
    {
        List<TypeSignatureParameter> newParameterTypeList = new ArrayList<>();
        for (TypeSignatureParameter parameter : typeSignatureParameterList) {
            if (parameter.isLongLiteral()) {
                newParameterTypeList.add(parameter);
                continue;
            }

            boolean isNamedTypeSignature = parameter.isNamedTypeSignature();
            TypeSignature parameterTypeSignature;
            // If it's a named type signatures only in the case of row signature types.
            if (isNamedTypeSignature) {
                parameterTypeSignature = parameter.getNamedTypeSignature().getTypeSignature();
            }
            else {
                parameterTypeSignature = parameter.getTypeSignature();
            }

            if (parameterTypeSignature.getParameters().isEmpty()) {
                if (changeTypeToVariable(typeSignature.getBase(), parameter)) {
                    newParameterTypeList.add(
                            TypeSignatureParameter.of(parameterTypeSignature.getBase()));
                }
                else {
                    if (isNamedTypeSignature) {
                        newParameterTypeList.add(TypeSignatureParameter.of(parameter.getNamedTypeSignature()));
                    }
                    else {
                        newParameterTypeList.add(TypeSignatureParameter.of(parameterTypeSignature));
                    }
                }
            }
            else {
                TypeSignature newTypeSignature =
                        new TypeSignature(
                                parameterTypeSignature.getBase(),
                                getTypeSignatureParameters(
                                        parameterTypeSignature.getStandardTypeSignature(),
                                        parameterTypeSignature.getParameters()));
                if (isNamedTypeSignature) {
                    newParameterTypeList.add(
                            TypeSignatureParameter.of(
                                    new NamedTypeSignature(
                                            Optional.empty(),
                                            newTypeSignature)));
                }
                else {
                    newParameterTypeList.add(TypeSignatureParameter.of(newTypeSignature));
                }
            }
        }
        return newParameterTypeList;
    }

    private static boolean changeTypeToVariable(String typeBase, TypeSignatureParameter parameter)
    {
        return (typeBase.equals(StandardTypes.DECIMAL)) || (typeBase.equals(VARCHAR) && parameter.isTypeSignature());
    }

    public static List<SqlInvokedFunction> deDuplicateFunctions(List<SqlInvokedFunction> sqlInvokedFunctions)
    {
        List<SqlInvokedFunction> result = new ArrayList<>();
        for (SqlInvokedFunction current : sqlInvokedFunctions) {
            boolean matched = false;
            for (int i = 0; i < result.size(); i++) {
                SqlInvokedFunction existing = result.get(i);

                if (functionsWithSameShape(existing, current)) {
                    // If a duplicate is found, choose the parameterized one
                    result.set(i, chooseParameterizedSignature(existing, current));
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                result.add(current);
            }
        }
        return ImmutableList.copyOf(result);
    }

    private static boolean functionsWithSameShape(SqlInvokedFunction left, SqlInvokedFunction right)
    {
        List<TypeSignature> leftArgTypes = left.getSignature().getArgumentTypes();
        List<TypeSignature> rightArgTypes = right.getSignature().getArgumentTypes();
        if (leftArgTypes.size() != rightArgTypes.size()) {
            return false;
        }

        // Recursively compare the container types
        for (int i = 0; i < leftArgTypes.size(); i++) {
            if (!functionsWithSameShape(leftArgTypes.get(i), rightArgTypes.get(i))) {
                return false;
            }
        }

        // The args types have the same shape, now compare the return type
        return functionsWithSameShape(left.getSignature().getReturnType(), right.getSignature().getReturnType());
    }

    private static boolean functionsWithSameShape(TypeSignature left, TypeSignature right)
    {
        if (!left.getBase().equals(right.getBase())) {
            return false;
        }

        if (left.getParameters().size() != right.getParameters().size()) {
            return false;
        }

        for (int i = 0; i < left.getParameters().size(); i++) {
            // Only check if its KIND is NAMED_TYPE or TYPE which is the case for container types.
            TypeSignatureParameter p1 = left.getParameters().get(i);
            TypeSignatureParameter p2 = right.getParameters().get(i);

            if ((p1.isTypeSignature() || p1.isNamedTypeSignature()) &&
                    (p2.isTypeSignature() || p2.isNamedTypeSignature())) {
                if (p1.getKind() != p2.getKind()) {
                    return false;
                }
                if (!functionsWithSameShape(
                        p1.getTypeSignatureOrNamedTypeSignature().get(),
                        p2.getTypeSignatureOrNamedTypeSignature().get())) {
                    return false;
                }
            }
        }
        return true;
    }

    private static SqlInvokedFunction chooseParameterizedSignature(SqlInvokedFunction existing, SqlInvokedFunction current)
    {
        // If the function shapes are same, choose the parameterized signature else return any one
        boolean existingParameterized = hasParameterizedType(existing.getSignature().getReturnType());
        boolean currentParameterized = hasParameterizedType(current.getSignature().getReturnType());

        if (existingParameterized || currentParameterized) {
            return existingParameterized ? existing : current;
        }
        return existing;
    }

    private static boolean hasParameterizedType(TypeSignature signature)
    {
        if (PARAMETERIZED_TYPES.contains(signature.getBase())) {
            checkArgument(signature.getParameters().size() == 1);
            TypeSignatureParameter parameter = signature.getParameters().get(0);
            return parameter.isVariable(); // for unbounded varchar, this would be a LONG literal.
        }

        // Recursive case for container types
        for (TypeSignatureParameter p : signature.getParameters()) {
            // Only check if its KIND is NAMED_TYPE or TYPE which is the case for container types.
            if (p.isTypeSignature() || p.isNamedTypeSignature()) {
                if (hasParameterizedType(p.getTypeSignatureOrNamedTypeSignature().get())) {
                    return true;
                }
            }
        }
        return false;
    }
}
