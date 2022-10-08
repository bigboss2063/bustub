//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> guard(latch_);
  auto new_lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  lock_table_[rid].request_queue_.emplace_back(new_lock_request);
  while (NeedWait(txn, rid, LockMode::SHARED)) {
    lock_table_[rid].cv_.wait(guard);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  for (auto &lock_request : lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ == txn->GetTransactionId()) {
      lock_request.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> guard(latch_);
  auto new_lock_request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_table_[rid].request_queue_.emplace_back(new_lock_request);
  while (NeedWait(txn, rid, LockMode::EXCLUSIVE)) {
    lock_table_[rid].cv_.wait(guard);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  for (auto &lock_request : lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ == txn->GetTransactionId()) {
      lock_request.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED || !txn->IsSharedLocked(rid)) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  std::unique_lock<std::mutex> guard(latch_);
  for (auto &lock_request : lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ == txn->GetTransactionId()) {
      lock_request.granted_ = false;
      lock_request.lock_mode_ = LockMode::EXCLUSIVE;
      txn->GetSharedLockSet()->erase(rid);
    }
  }
  while (NeedWait(txn, rid, LockMode::EXCLUSIVE)) {
    lock_table_[rid].cv_.wait(guard);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  for (auto &lock_request : lock_table_[rid].request_queue_) {
    if (lock_request.txn_id_ == txn->GetTransactionId()) {
      lock_request.granted_ = true;
    }
  }
  txn->SetState(TransactionState::GROWING);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> guard(latch_);
  if (!txn->IsExclusiveLocked(rid) && !txn->IsSharedLocked(rid)) {
    return false;
  }
  auto iter = lock_table_[rid].request_queue_.begin();
  bool is_found = false;
  while (iter != lock_table_[rid].request_queue_.end()) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      is_found = true;
      lock_table_[rid].request_queue_.erase(iter);
      lock_table_[rid].cv_.notify_all();
      break;
    }
    iter++;
  }
  if (!is_found) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetExclusiveLockSet()->erase(rid);
  txn->GetSharedLockSet()->erase(rid);
  return true;
}
bool LockManager::NeedWait(Transaction *txn, const RID &rid, LockMode lock_mode) {
  bool need_wait = false;
  bool occur_abort = false;
  auto request_queue = lock_table_[rid].request_queue_;
  for (auto iter = request_queue.begin(); iter->txn_id_ != txn->GetTransactionId(); iter++) {
    if (lock_mode == LockMode::SHARED) {
      if (iter->granted_ && iter->lock_mode_ == LockMode::EXCLUSIVE) {
        need_wait = true;
      }
    } else {
      if (iter->granted_) {
        need_wait = true;
      }
    }
  }
  if (!need_wait) {
    return need_wait;
  }
  for (auto iter = request_queue.begin(); iter->txn_id_ != txn->GetTransactionId(); iter++) {
    if (iter->txn_id_ > txn->GetTransactionId()) {
      if (lock_mode == LockMode::SHARED) {
        if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
          TransactionManager::GetTransaction(iter->txn_id_)->SetState(TransactionState::ABORTED);
          occur_abort = true;
        }
      } else {
        TransactionManager::GetTransaction(iter->txn_id_)->SetState(TransactionState::ABORTED);
        occur_abort = true;
      }
      continue;
    }
    if (request_queue.back().lock_mode_ == LockMode::EXCLUSIVE) {
      need_wait = true;
    }
    if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
      need_wait = true;
    }
  }
  if (occur_abort) {
    lock_table_[rid].cv_.notify_all();
  }
  return need_wait;
}

}  // namespace bustub
