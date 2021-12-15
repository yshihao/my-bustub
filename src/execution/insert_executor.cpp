//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  tableMeta = catalog->GetTable(plan_->TableOid());
  valuePos = 0;
  indexVector = exec_ctx_->GetCatalog()->GetTableIndexes(tableMeta->name_);
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::insertTable(Tuple *tuple, RID *rid, Transaction *transactioin) {
  bool isSuccess = tableMeta->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
  valuePos++;
  if (transactioin->GetState() == TransactionState::ABORTED && GetExecutorContext()->GetLockManager() != nullptr) {
    TransactionManager *transactionManager = GetExecutorContext()->GetTransactionManager();
    transactionManager->Abort(GetExecutorContext()->GetTransaction());
    return false;
  }
  // TableWriteRecord tableWrite(*rid, WType::INSERT, *tuple, tableMeta->table_.get());
  if (isSuccess) {
    // 每次插入成功 都需要更新所有索引
    // GetExecutorContext()->GetTransaction()->AppendTableWriteRecord(tableWrite);
    for (auto indexInfoP : indexVector) {
      Tuple keyTuple =
          tuple->KeyFromTuple(tableMeta->schema_, indexInfoP->key_schema_, indexInfoP->index_->GetKeyAttrs());
      indexInfoP->index_->InsertEntry(keyTuple, *rid, exec_ctx_->GetTransaction());
    }
  }
  return isSuccess;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 插入失败 则返回false
  if (plan_->IsRawInsert()) {
    std::vector<std::vector<Value>> vectorValues = plan_->RawValues();
    if (valuePos >= static_cast<int32_t>(vectorValues.size())) {
      return false;
    }
    Tuple mytuple(vectorValues[valuePos], &tableMeta->schema_);
    return insertTable(&mytuple, rid, exec_ctx_->GetTransaction());
  }
  // 数据从孩子节点传
  // const AbstractPlanNode *abstractPlanNode = plan_->GetChildPlan();
  // std::shared_ptr<AbstractExecutor> abstractExecutor;
  // if (abstractPlanNode->GetType() == PlanType::SeqScan) {
  //   abstractExecutor = std::make_shared<SeqScanExecutor>(exec_ctx_, dynamic_cast<const SeqScanPlanNode
  //   *>(abstractPlanNode));
  // } else if (abstractPlanNode->GetType() == PlanType::IndexScan) {
  //   abstractExecutor = std::make_shared<IndexScanExecutor>(exec_ctx_, dynamic_cast<const IndexScanPlanNode
  //   *>(abstractPlanNode));
  // } else {
  //   return false;
  // }
  Tuple mytuple;
  if (child_executor_->Next(&mytuple, rid)) {
    return insertTable(&mytuple, rid, exec_ctx_->GetTransaction());
  }

  return false;
}

}  // namespace bustub
