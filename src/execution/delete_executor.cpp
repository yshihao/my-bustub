//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  indexVector = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
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
        // 需要return 嘛
        return false;
      }
    }

    // TableWriteRecord tableWrite(*rid, WType::DELETE, mytuple, table_info_->table_.get());
    bool isSuccess = table_info_->table_->MarkDelete(*(rid), exec_ctx_->GetTransaction());
    if (isSuccess) {
      // transaction->AppendTableWriteRecord(tableWrite);
      // 索引不能在这里删除！！！
      for (auto indexInfoP : indexVector) {
        Tuple keyOldTuple =
            mytuple.KeyFromTuple(table_info_->schema_, indexInfoP->key_schema_, indexInfoP->index_->GetKeyAttrs());
        indexInfoP->index_->DeleteEntry(keyOldTuple, *(rid), exec_ctx_->GetTransaction());
        IndexWriteRecord indexWrite(*rid, table_info_->oid_, WType::DELETE, keyOldTuple, indexInfoP->index_oid_,
                                    exec_ctx_->GetCatalog());
        transaction->AppendTableWriteRecord(indexWrite);
      }
      return true;
    }
  }

  return false;
}

}  // namespace bustub
