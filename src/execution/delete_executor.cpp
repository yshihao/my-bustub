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
    bool isSuccess = table_info_->table_->MarkDelete(*(rid), exec_ctx_->GetTransaction());
    if (isSuccess) {
      for (auto indexInfoP : indexVector) {
        Tuple keyOldTuple =
            mytuple.KeyFromTuple(table_info_->schema_, indexInfoP->key_schema_, indexInfoP->index_->GetKeyAttrs());
        indexInfoP->index_->DeleteEntry(keyOldTuple, *(rid), exec_ctx_->GetTransaction());
      }
      return true;
    }
  }

  return false;
}

}  // namespace bustub
