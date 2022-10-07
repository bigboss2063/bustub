//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), count_(0) {}

void LimitExecutor::Init() { child_executor_->Init(); }

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }
  count_++;
  return count_ <= plan_->GetLimit();
}

}  // namespace bustub
