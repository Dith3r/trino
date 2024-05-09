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
package io.trino.sql.planner.iterative.rule;

import com.google.inject.Inject;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;

import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

/**
 * Controls which implementation of distinct aggregation should be used for particular {@link AggregationNode}
 */
public class DistinctAggregationController
{
    private static final int MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = 8;
    private static final int PRE_AGGREGATE_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * 8;

    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public DistinctAggregationController(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    public boolean shouldAddMarkDistinct(AggregationNode aggregationNode, Rule.Context context)
    {
        return !canParallelizeSingleStepDistinctAggregation(aggregationNode, context, MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER);
    }

    public boolean shouldUsePreAggregate(AggregationNode aggregationNode, Rule.Context context)
    {
        if (canParallelizeSingleStepDistinctAggregation(aggregationNode, context, PRE_AGGREGATE_MAX_OUTPUT_ROW_COUNT_MULTIPLIER)) {
            return false;
        }

        // mark-distinct is better than pre-aggregate if the number of group-by keys is bigger than 2
        // because group-by keys are added to every grouping set and this makes partial aggregation behaves badly
        return aggregationNode.getGroupingKeys().size() <= 2;
    }

    private boolean canParallelizeSingleStepDistinctAggregation(AggregationNode aggregationNode, Rule.Context context, int maxOutputRowCountMultiplier)
    {
        if (aggregationNode.getGroupingKeys().isEmpty()) {
            // global distinct aggregation is computed using a single thread. MarkDistinct will help parallelize the execution.
            return false;
        }
        double numberOfDistinctValues = getMinDistinctValueCountEstimate(aggregationNode, context);
        if (Double.isNaN(numberOfDistinctValues)) {
            // if the estimate is unknown, use MarkDistinct to avoid query failure
            return false;
        }
        int maxNumberOfConcurrentThreadsForAggregation = getMaxNumberOfConcurrentThreadsForAggregation(context);

        if (numberOfDistinctValues <= maxOutputRowCountMultiplier * maxNumberOfConcurrentThreadsForAggregation) {
            // small numberOfDistinctValues reduces the distinct aggregation parallelism, also because the partitioning may be skewed.
            // This makes query to underutilize the cluster CPU but also to possibly concentrate memory on few nodes.
            // MarkDistinct should increase the parallelism at a cost of CPU.
            return false;
        }

        // can parallelize single-step, and single-step distinct is more efficient than alternatives
        return true;
    }

    private int getMaxNumberOfConcurrentThreadsForAggregation(Rule.Context context)
    {
        return taskCountEstimator.estimateHashedTaskCount(context.getSession()) * getTaskConcurrency(context.getSession());
    }

    private double getMinDistinctValueCountEstimate(AggregationNode aggregationNode, Rule.Context context)
    {
        // NDV stats for multiple grouping keys are unreliable, let's pick a conservative lower bound by taking maximum NDV for all grouping keys.
        // this assumes that grouping keys are 100% correlated.
        // in the case of a lower correlation, the NDV can only be higher.
        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(aggregationNode.getSource());
        return aggregationNode.getGroupingKeys()
                .stream()
                .filter(symbol -> !isNaN(sourceStats.getSymbolStatistics(symbol).getDistinctValuesCount()))
                .map(symbol -> sourceStats.getSymbolStatistics(symbol).getDistinctValuesCount())
                .max(Double::compareTo).orElse(Double.NaN);
    }
}
