//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  if (plan_->IsRawInsert()) {
    iter_ = plan_->RawValues().begin();
  } else {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    if (iter_ == plan_->RawValues().end()) {
      return false;
    }
    *tuple = Tuple(*iter_, &table_info_->schema_);
    *rid = tuple->GetRid();
    iter_++;
  } else {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
  }
  if (!table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (const auto &index : indexes) {
    Tuple index_tuple = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->InsertEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
  }
  return Next(tuple, rid);
}

}  // namespace bustub
