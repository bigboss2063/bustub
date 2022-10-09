//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
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
    Tuple updated_tuple = GenerateUpdatedTuple(*tuple);
    if (!table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction())) {
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
          txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (lock_manager != nullptr && !lock_manager->Unlock(txn, *rid)) {
          return false;
        }
      }
      return false;
    }
    // 这个代码中有注释写了只有 update 才用得到 tuple 字段，所以 insert 和 delete 中就不要插入这个了
    txn->AppendTableWriteRecord(TableWriteRecord(*rid, WType::UPDATE, *tuple, table_info_->table_.get()));
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (const auto &index : indexes) {
      Tuple old_index_tuple =
          tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      // 删除的 index_tuple 应该根据更新前的来生成
      index->index_->DeleteEntry(old_index_tuple, *rid, exec_ctx_->GetTransaction());
      Tuple updated_index_tuple =
          updated_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(updated_index_tuple, *rid, exec_ctx_->GetTransaction());
      // tuple 字段的描述是用于构造一个 index tuple，所以传入的应该 table tuple，一定要注意
      auto index_write_record = IndexWriteRecord(*rid, table_info_->oid_, WType::UPDATE, updated_tuple,
                                                 index->index_oid_, exec_ctx_->GetCatalog());
      index_write_record.old_tuple_ = *tuple;
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

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
