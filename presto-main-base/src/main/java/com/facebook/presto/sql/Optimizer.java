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
package com.facebook.presto.sql;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizerResult;
import com.facebook.presto.sql.planner.sanity.PlanChecker;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getQueryAnalyzerTimeout;
import static com.facebook.presto.SystemSessionProperties.isPrintStatsForNonJoinQuery;
import static com.facebook.presto.SystemSessionProperties.isVerboseOptimizerInfoEnabled;
import static com.facebook.presto.SystemSessionProperties.isVerboseOptimizerResults;
import static com.facebook.presto.common.RuntimeMetricName.VALIDATE_FINAL_PLAN_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.VALIDATE_INTERMEDIATE_PLAN_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_PLANNING_TIMEOUT;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.sql.OptimizerRuntimeTrackUtil.getOptimizerNameForLog;
import static com.facebook.presto.sql.OptimizerRuntimeTrackUtil.trackOptimizerRuntime;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Optimizer
{
    public enum PlanStage
    {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final List<PlanOptimizer> planOptimizers;
    private final PlanChecker planChecker;
    private final Session session;
    private final Metadata metadata;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final WarningCollector warningCollector;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final boolean explain;

    public Optimizer(
            Session session,
            Metadata metadata,
            List<PlanOptimizer> planOptimizers,
            PlanChecker planChecker,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            boolean explain)
    {
        this.session = requireNonNull(session, "session is null");
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.explain = explain;
    }

    public Plan validateAndOptimizePlan(PlanNode root, PlanStage stage)
    {
        validateIntermediatePlanWithRuntimeStats(root);

        boolean enableVerboseRuntimeStats = SystemSessionProperties.isVerboseRuntimeStatsEnabled(session);
        if (stage.ordinal() >= OPTIMIZED.ordinal()) {
            for (PlanOptimizer optimizer : planOptimizers) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new PrestoException(QUERY_PLANNING_TIMEOUT, String.format("The query optimizer exceeded the timeout of %s.", getQueryAnalyzerTimeout(session).toString()));
                }
                long start = System.nanoTime();
                PlanOptimizerResult optimizerResult = optimizer.optimize(root, session, TypeProvider.viewOf(variableAllocator.getVariables()), variableAllocator, idAllocator, warningCollector);
                requireNonNull(optimizerResult, format("%s returned a null plan", optimizer.getClass().getName()));
                if (enableVerboseRuntimeStats || trackOptimizerRuntime(session, optimizer)) {
                    session.getRuntimeStats().addMetricValue(String.format("optimizer%sTimeNanos", getOptimizerNameForLog(optimizer)), NANO, System.nanoTime() - start);
                }
                TypeProvider types = TypeProvider.viewOf(variableAllocator.getVariables());

                collectOptimizerInformation(optimizer, root, optimizerResult, types);
                root = optimizerResult.getPlanNode();
            }
        }
        if (stage.ordinal() >= OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            validateFinalPlanWithRuntimeStats(root);
        }

        TypeProvider types = TypeProvider.viewOf(variableAllocator.getVariables());
        return new Plan(root, types, computeStats(root, types));
    }

    private void validateIntermediatePlanWithRuntimeStats(PlanNode root)
    {
        session.getRuntimeStats().recordWallAndCpuTime(VALIDATE_INTERMEDIATE_PLAN_TIME_NANOS,
                () -> planChecker.validateIntermediatePlan(root, session, metadata, warningCollector));
    }

    private void validateFinalPlanWithRuntimeStats(PlanNode root)
    {
        session.getRuntimeStats().recordWallAndCpuTime(VALIDATE_FINAL_PLAN_TIME_NANOS,
                () -> planChecker.validateFinalPlan(root, session, metadata, warningCollector));
    }

    private StatsAndCosts computeStats(PlanNode root, TypeProvider types)
    {
        if (explain || isPrintStatsForNonJoinQuery(session) ||
                PlanNodeSearcher.searchFrom(root).where(node ->
                        (node instanceof JoinNode) || (node instanceof SemiJoinNode)).matches()) {
            StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
            CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
            return StatsAndCosts.create(root, statsProvider, costProvider, session);
        }
        return StatsAndCosts.empty();
    }

    private void collectOptimizerInformation(PlanOptimizer optimizer, PlanNode oldNode, PlanOptimizerResult planOptimizerResult, TypeProvider types)
    {
        if (optimizer instanceof IterativeOptimizer) {
            // iterative optimizers do their own recording of what rules got triggered
            return;
        }

        String optimizerName = getOptimizerNameForLog(optimizer);
        boolean isTriggered = planOptimizerResult.isOptimizerTriggered();
        boolean isApplicable =
                isTriggered ||
                        !optimizer.isEnabled(session) && isVerboseOptimizerInfoEnabled(session) &&
                                optimizer.isApplicable(oldNode, session, TypeProvider.viewOf(variableAllocator.getVariables()), variableAllocator, idAllocator, warningCollector);
        boolean isCostBased = isTriggered && optimizer.isCostBased(session);
        String statsSource = optimizer.getStatsSource();

        if (isTriggered || isApplicable || isCostBased) {
            session.getOptimizerInformationCollector().addInformation(
                    new PlanOptimizerInformation(optimizerName, isTriggered, Optional.of(isApplicable), Optional.empty(), Optional.of(isCostBased), statsSource == null ? Optional.empty() : Optional.of(statsSource)));
        }

        if (isTriggered && isVerboseOptimizerResults(session, optimizerName)) {
            String oldNodeStr = PlannerUtils.getPlanString(oldNode, session, types, metadata, false);
            String newNodeStr = PlannerUtils.getPlanString(planOptimizerResult.getPlanNode(), session, types, metadata, false);
            session.getOptimizerResultCollector().addOptimizerResult(optimizerName, oldNodeStr, newNodeStr);
        }
    }
}
