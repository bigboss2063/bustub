//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  auto left_expression = plan_->LeftJoinKeyExpression();
  Tuple tuple;
  RID rid;
  // build hash table
  while (left_child_->Next(&tuple, &rid)) {
    auto left_join_key = left_expression->Evaluate(&tuple, left_child_->GetOutputSchema());
    auto hash_key = HashUtil::HashValue(&left_join_key);
    hash_table_[hash_key].emplace_back(tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;
  auto right_expression = plan_->RightJoinKeyExpression();
  while (right_child_->Next(&right_tuple, &right_rid)) {
    auto right_join_key = right_expression->Evaluate(&right_tuple, right_child_->GetOutputSchema());
    auto hash_key = HashUtil::HashValue(&right_join_key);
    if (hash_table_.count(hash_key) != 0) {
      for (const auto &left_tuple : hash_table_[hash_key]) {
        std::vector<Value> values;
        for (const auto &columns : plan_->OutputSchema()->GetColumns()) {
          values.emplace_back(columns.GetExpr()->EvaluateJoin(&left_tuple, left_child_->GetOutputSchema(), &right_tuple,
                                                              right_child_->GetOutputSchema()));
        }
        result_.emplace_back(Tuple(values, plan_->OutputSchema()));
      }
    }
  }
  if (!result_.empty()) {
    *tuple = result_.front();
    *rid = tuple->GetRid();
    result_.pop_front();
    return true;
  }
  return false;
}

}  // namespace bustub
