//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() { child_->Init(); }

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.Begin()) {
    while (child_->Next(tuple, rid)) {
      auto aggregate_key = MakeAggregateKey(tuple);
      auto aggregate_value = MakeAggregateValue(tuple);
      aht_.InsertCombine(aggregate_key, aggregate_value);
    }
    aht_iterator_ = aht_.Begin();
  }
  while (plan_->GetHaving() != nullptr && aht_iterator_ != aht_.End() &&
         !plan_->GetHaving()
              ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
              .GetAs<bool>()) {
    ++aht_iterator_;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  for (const auto &column : plan_->OutputSchema()->GetColumns()) {
    values.emplace_back(
        column.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  ++aht_iterator_;
  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
