//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), table_info_(nullptr) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  while (child_executor_->Next(tuple, rid)) {
    if (lock_manager != nullptr && !txn->IsExclusiveLocked(*rid)) {
      if (txn->IsSharedLocked(*rid)) {
        if (!lock_manager->LockUpgrade(txn, *rid)) {
          return false;
        }
      } else {
        if (!lock_manager->LockExclusive(txn, *rid)) {
          return false;
        }
      }
    }
    if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
          txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (lock_manager != nullptr && !lock_manager->Unlock(txn, *rid)) {
          return false;
        }
      }
      return false;
    }
    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      auto index_tuple = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
      auto index_write_record = IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, *tuple, index->index_oid_,
                                                 exec_ctx_->GetCatalog());
      txn->AppendTableWriteRecord(index_write_record);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
        txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_manager != nullptr && !lock_manager->Unlock(txn, *rid)) {
        return false;
      }
    }
  }
  return false;
}

}  // namespace bustub
