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
package com.facebook.presto.sidecar;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.ml.MLPlugin;
import com.facebook.presto.ml.type.ClassifierParametricType;
import com.facebook.presto.mongodb.MongoPlugin;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestNativeSidecarPluginLoadedTypesAndFunctions
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .build();
        setupNativeSidecarPlugin(queryRunner);
        queryRunner.installPlugin(new MongoPlugin());
        queryRunner.installPlugin(new MLPlugin());
        return queryRunner;
    }

    @Test
    public void testTypesLoaded()
    {
        FunctionAndTypeManager functionAndTypeManager = getQueryRunner().getMetadata().getFunctionAndTypeManager();
        assertTrue(functionAndTypeManager.hasType(OBJECT_ID.getTypeSignature()));
        // parametric type
        TypeSignature classifierSignature = new TypeSignature(
                ClassifierParametricType.NAME,
                TypeSignatureParameter.of(BIGINT.getTypeSignature()));
        assertTrue(functionAndTypeManager.hasType(classifierSignature));
    }

    @Test
    public void testFunctionsLoaded()
    {
        // ML plugin functions
        MaterializedResult functions = computeActual("SHOW FUNCTIONS LIKE 'classify'");
        assertTrue(functions.getMaterializedRows().stream()
                .anyMatch(row -> row.getField(0).equals("classify") && row.getField(3).equals("scalar")));

        FunctionAndTypeManager functionAndTypeManager = getQueryRunner().getMetadata().getFunctionAndTypeManager();

        Collection<? extends SqlFunction> nativeClassifyFunctions = functionAndTypeManager
                .getBuiltInPluginFunctionNamespaceManager()
                .getFunctions(Optional.empty(), QualifiedObjectName.valueOf(functionAndTypeManager.getDefaultNamespace(), "classify"));
        assertFalse(nativeClassifyFunctions.isEmpty());

        List<? extends SqlFunction> mongoCastOperators = functionAndTypeManager.listOperators().stream()
                .filter(function -> function.getSignature().getName().getCatalogSchemaName().equals(JAVA_BUILTIN_NAMESPACE))
                .filter(function -> function.getSignature().getArgumentTypes().size() == 1)
                .filter(function -> function.getSignature().getArgumentTypes().get(0).equals(OBJECT_ID.getTypeSignature()))
                .filter(function -> function.getSignature().getReturnType().toString().startsWith("varchar"))
                .collect(toImmutableList());

        assertFalse(mongoCastOperators.isEmpty());
    }
}
