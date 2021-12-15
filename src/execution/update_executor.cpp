//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  indexVector = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool UpdateExecutor::indexNeedUpdate(const IndexInfo *indexInfo) {
  // 返回值就是引用
  std::vector<uint32_t> indexKeyAttrs = indexInfo->index_->GetMetadata()->GetKeyAttrs();
  const std::unordered_map<uint32_t, UpdateInfo> *updateKey = plan_->GetUpdateAttr();
  for (auto &attr : indexKeyAttrs) {
    if (updateKey->find(attr) != updateKey->end()) {
      return true;
    }
  }
  return false;
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple mytuple;
  if (child_executor_->Next(&mytuple, rid)) {
    LockManager *lockManager = GetExecutorContext()->GetLockManager();
    Transaction *transaction = GetExecutorContext()->GetTransaction();
    if (lockManager != nullptr) {
      bool isLock = false;
      if (transaction->IsSharedLocked(*rid)) {
        isLock = lockManager->LockUpgrade(transaction, *rid);
      } else {
        isLock = lockManager->LockExclusive(transaction, *rid);
      }
      if (!isLock) {
        TransactionManager *transactionManager = GetExecutorContext()->GetTransactionManager();
        transactionManager->Abort(GetExecutorContext()->GetTransaction());
        return false;
      }
    }

    Tuple newTuple = GenerateUpdatedTuple(mytuple);
    // 在更新事务的时候 就已经 修改了这个事务的writeset
    bool isSuccess = table_info_->table_->UpdateTuple(newTuple, *(rid), exec_ctx_->GetTransaction());
    if (isSuccess) {
      // // 需要更新成功嘛？
      // TableWriteRecord tableWrite(*rid, WType::UPDATE, mytuple, table_info_->table_.get());
      // transaction->AppendTableWriteRecord(tableWrite);
      for (auto indexInfoP : indexVector) {
        // 索引更新 需要重新删除后 再插入
        if (indexNeedUpdate(indexInfoP)) {
          Tuple keyOldTuple =
              mytuple.KeyFromTuple(table_info_->schema_, indexInfoP->key_schema_, indexInfoP->index_->GetKeyAttrs());
          indexInfoP->index_->DeleteEntry(keyOldTuple, *rid, exec_ctx_->GetTransaction());
          Tuple keyNewTuple =
              newTuple.KeyFromTuple(table_info_->schema_, indexInfoP->key_schema_, indexInfoP->index_->GetKeyAttrs());
          indexInfoP->index_->InsertEntry(keyNewTuple, *rid, exec_ctx_->GetTransaction());
        }
      }
      return true;
    }
  }
  return false;
}
}  // namespace bustub
