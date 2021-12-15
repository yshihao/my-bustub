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

bool LockManager::isAllSharedLock(const std::list<LockRequest> &lockList) {
  for (auto &lockRequest : lockList) {
    if (lockRequest.lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
  }
  return true;
}
// 要对事务的状态进行修改
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);
  // 保证二阶段锁的性质
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  // roll back 才返回false
  // 不用考虑隔离级别
  LockRequest lockRequest(txn->GetTransactionId(), LockMode::SHARED);
  // 会构造一个空的对象
  lock_table_[rid].request_queue_.emplace_back(lockRequest);
  txn->SetState(TransactionState::GROWING);
  auto lockIterator = lock_table_.find(rid);
  assert(lockIterator != lock_table_.end());
  if (lockIterator->second.has_exclusive) {
    lockIterator->second.cv_.wait(lck, [&]() {
      return txn->GetState() == TransactionState::ABORTED ||
             (!lockIterator->second.upgrading_ && !lockIterator->second.has_exclusive);
    });
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  for (auto &lockRequest : lockIterator->second.request_queue_) {
    if (lockRequest.txn_id_ == txn->GetTransactionId()) {
      lockRequest.granted_ = true;
      break;
    }
  }
  lockIterator->second.shared_count++;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  LockRequest lockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_table_[rid].request_queue_.emplace_back(lockRequest);
  txn->SetState(TransactionState::GROWING);
  // 关联性容器  除非对迭代器删除 否则不会失效！
  auto lockIterator = lock_table_.find(rid);
  if (lockIterator->second.has_exclusive || lockIterator->second.shared_count > 0) {
    lockIterator->second.cv_.wait(lck, [&]() {
      return txn->GetState() == TransactionState::ABORTED ||
             (!lockIterator->second.upgrading_ && !lockIterator->second.has_exclusive &&
              lockIterator->second.shared_count == 0);
    });
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  for (auto &lockRequest : lockIterator->second.request_queue_) {
    if (lockRequest.txn_id_ == txn->GetTransactionId()) {
      lockRequest.granted_ = true;
      break;
    }
  }
  lockIterator->second.has_exclusive = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  auto lockIterator = lock_table_.find(rid);
  if (lockIterator == lock_table_.end()) {
    return true;
  }
  // 另一个事务在等着更新这个锁 隐含的abort
  if (lockIterator->second.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  lockIterator->second.upgrading_ = true;
  for (auto &lockRequest : lockIterator->second.request_queue_) {
    if (lockRequest.txn_id_ == txn->GetTransactionId()) {
      lockRequest.lock_mode_ = LockMode::EXCLUSIVE;
      lockRequest.granted_ = false;
      lockIterator->second.shared_count--;
      txn->GetSharedLockSet()->erase(rid);
      break;
    }
  }
  if (lockIterator->second.has_exclusive || lockIterator->second.shared_count > 0) {
    lockIterator->second.cv_.wait(lck, [&]() {
      return txn->GetState() == TransactionState::ABORTED ||
             (lockIterator->second.shared_count == 0 && !lockIterator->second.has_exclusive);
    });
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    lockIterator->second.upgrading_ = false;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  for (auto &lockRequest : lockIterator->second.request_queue_) {
    if (lockRequest.txn_id_ == txn->GetTransactionId()) {
      lockRequest.granted_ = true;
      break;
    }
  }
  lockIterator->second.has_exclusive = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  lockIterator->second.upgrading_ = false;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 与隔离级别有关嘛 ？
  // if (txn->GetState() == TransactionState::SHRINKING) {
  //   txn->SetState(TransactionState::ABORTED);
  //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UNLOCK_ON_SHRINKING);
  //   return false;
  // }
  std::unique_lock<std::mutex> lck(latch_);
  auto lockIterator = lock_table_.find(rid);
  if (lockIterator == lock_table_.end()) {
    return true;
  }
  LockMode lockMode = LockMode::EXCLUSIVE;
  for (auto &lockRequest : lockIterator->second.request_queue_) {
    if (lockRequest.txn_id_ == txn->GetTransactionId()) {
      if (lockRequest.lock_mode_ == LockMode::EXCLUSIVE) {
        txn->GetExclusiveLockSet()->erase(rid);
        lockIterator->second.has_exclusive = false;
      } else {
        lockMode = LockMode::SHARED;
        txn->GetSharedLockSet()->erase(rid);
        lockIterator->second.shared_count--;
      }
      break;
    }
  }
  std::list<LockRequest> &lockList = lockIterator->second.request_queue_;
  lockList.erase(std::remove_if(lockList.begin(), lockList.end(),
                                [txn](LockRequest locks) { return locks.txn_id_ == txn->GetTransactionId(); }),
                 lockList.end());
  // 如果是读已 提交 不会改变事务状态嘛？
  if (txn->GetState() == TransactionState::GROWING &&
      !(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lockMode == LockMode::SHARED)) {
    txn->SetState(TransactionState::SHRINKING);
  }
  if (!lockIterator->second.has_exclusive) {
    lockIterator->second.cv_.notify_all();
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // LOG_INFO("t1 %d, t2 %d\n",t1, t2);
  mytransactions[t1]++;
  auto Iterator = waits_for_.find(t1);
  if (Iterator == waits_for_.end()) {
    std::vector<txn_id_t> tmp{t2};
    waits_for_.emplace(t1, tmp);
    return;
  }
  Iterator->second.push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  mytransactions[t1]--;
  if (mytransactions[t1] == 0) {
    mytransactions.erase(t1);
  }
  auto Iterator = waits_for_.find(t1);
  if (Iterator != waits_for_.end()) {
    auto &txnVector = Iterator->second;
    txnVector.erase(std::remove_if(txnVector.begin(), txnVector.end(), [&](txn_id_t txn_id) { return txn_id == t2; }),
                    txnVector.end());
  }
}

bool LockManager::DfsSearch(const txn_id_t &txn_id) {
  auto Iterator = waits_for_.find(txn_id);
  if (Iterator == waits_for_.end()) {
    return false;
  }
  // 有向图 判环的方法
  for (auto &mytxn : Iterator->second) {
    if (route[mytxn]) {
      mytag = mytxn;
      myanswer = mytxn;
      return true;
    }
    if (vis[mytxn]) {
      continue;
    }
    // 为什么emplace 不行？
    vis[mytxn] = true;
    route[mytxn] = true;

    bool result = DfsSearch(mytxn);
    if (result && mytag != -1) {
      myanswer = std::max(myanswer, mytxn);
      if (mytxn == mytag) {
        mytag = -1;
      }
    }
    if (result) {
      return true;
    }
  }
  return false;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  // std::unique_lock<std::mutex> l(latch_);
  vis.clear();
  mytag = -1;
  myanswer = -1;
  for (auto mytxn_id : mytransactions) {
    if (vis.find(mytxn_id.first) != vis.end()) {
      continue;
    }
    route.clear();
    vis[mytxn_id.first] = true;

    route[mytxn_id.first] = true;
    // for (auto &mytmp : waits_for_) {
    //   LOG_INFO("edge source node %d ", mytmp.first);
    //   for (auto &tmp : mytmp.second) {
    //      LOG_INFO("edge dst node %d ", tmp);
    //   }
    // }
    if (DfsSearch(mytxn_id.first)) {
      *txn_id = myanswer;
      // 可能会多次调用
      return true;
    }
  }

  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto &wait_for : waits_for_) {
    for (auto &txn_id : wait_for.second) {
      result.emplace_back(wait_for.first, txn_id);
    }
  }
  return result;
}

void LockManager::breakCircle(const txn_id_t &txn_id) {
  auto ridIterator = txn_to_RID.find(txn_id);
  auto lockIterator = lock_table_.find(ridIterator->second);
  std::list<LockRequest> &lockList = lockIterator->second.request_queue_;
  lockList.erase(
      std::remove_if(lockList.begin(), lockList.end(), [txn_id](LockRequest locks) { return locks.txn_id_ == txn_id; }),
      lockList.end());
  for (auto &ele : waits_for_[txn_id]) {
    RemoveEdge(txn_id, ele);
  }
  waits_for_.erase(txn_id);
  for (auto &wait_pair : waits_for_) {
    while (std::find(wait_pair.second.begin(), wait_pair.second.end(), txn_id) != wait_pair.second.end()) {
      RemoveEdge(wait_pair.first, txn_id);
    }
  }
  // 一个RID上会有很多个 事务在请求！！！
  lockIterator->second.cv_.notify_all();
}
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      // continue;
      for (auto &mypair : lock_table_) {
        for (auto &lockRequest : mypair.second.request_queue_) {
          if (lockRequest.granted_) {
            continue;
          }
          // 在这条记录上 这个事务在等待锁。 会不会有事务在多条记录上等待？ 不会
          txn_to_RID.emplace(lockRequest.txn_id_, mypair.first);

          for (auto &lockRequest2 : mypair.second.request_queue_) {
            if (lockRequest2.granted_) {
              AddEdge(lockRequest.txn_id_, lockRequest2.txn_id_);
            }
          }

          // if (lockRequest.lock_mode_ == LockMode::EXCLUSIVE) {
          //   for (auto &lockRequest2 : mypair.second.request_queue_) {
          //     if (lockRequest2.granted_ && lockRequest2.lock_mode_ == LockMode::SHARED) {
          //       AddEdge(lockRequest.txn_id_, lockRequest2.txn_id_);
          //     }
          //   }
          // }
        }
      }
      // LOG_INFO("what!!");
      for (auto &mypair : waits_for_) {
        std::sort(mypair.second.begin(), mypair.second.end(), [](txn_id_t a, txn_id_t b) { return a > b; });
      }
      // std::sort(mytransactions.begin(), mytransactions.end(), [](const txn_id_t& a, const txn_id_t& b){
      //   return a > b;
      // });
      txn_id_t low_txn_id;
      while (HasCycle(&low_txn_id)) {
        Transaction *transaction = TransactionManager::GetTransaction(low_txn_id);
        transaction->SetState(TransactionState::ABORTED);
        breakCircle(low_txn_id);
      }
      mytransactions.clear();
      txn_to_RID.clear();
      waits_for_.clear();
    }
  }
}

}  // namespace bustub
