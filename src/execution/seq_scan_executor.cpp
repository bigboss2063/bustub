//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_iterator_(TableIterator(nullptr, RID(INVALID_PAGE_ID, 0), nullptr)) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (table_iterator_ != table_info_->table_->End() && plan_->GetPredicate() != nullptr &&
         !plan_->GetPredicate()->Evaluate(&(*table_iterator_), plan_->OutputSchema()).GetAs<bool>()) {
    table_iterator_++;
  }
  if (table_iterator_ == table_info_->table_->End()) {
    return false;
  }
  std::vector<Value> values;
  for (const auto &column : plan_->OutputSchema()->GetColumns()) {
    values.push_back(column.GetExpr()->Evaluate(&(*table_iterator_), &table_info_->schema_));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  *rid = table_iterator_->GetRid();
  table_iterator_++;
  return true;
}

}  // namespace bustub
