//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Value> values;
    hash_t hash_key = 0;
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      Value value = tuple->GetValue(plan_->OutputSchema(), plan_->OutputSchema()->GetColIdx(column.GetName()));
      HashUtil::CombineHashes(hash_key, HashUtil::HashValue(&value));
      values.emplace_back(value);
    }
    if (!IsRepeat(values, hash_key)) {
      distinct_map_[hash_key].emplace_back(values);
      *tuple = Tuple(values, plan_->OutputSchema());
      return true;
    }
  }
  return false;
}
bool DistinctExecutor::IsRepeat(std::vector<Value> values, hash_t hash_key) {
  auto iter = distinct_map_.find(hash_key);
  if (iter == distinct_map_.end()) {
    return false;
  }
  for (auto &rows : iter->second) {
    bool is_repeat = true;
    for (size_t i = 0; i < rows.size(); i++) {
      if (values[i].CompareEquals(rows[i]) != CmpBool::CmpTrue) {
        is_repeat = false;
        break;
      }
    }
    if (is_repeat) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
