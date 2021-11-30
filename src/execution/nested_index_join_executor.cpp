//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  // child 的执行器 会直接给你
  child_executor_->Init();
  tableMetadata = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  std::string tableName = tableMetadata->name_;
  innerIndexInfo = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), tableName);
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!result.empty() && ridIderator != result.end()) {
    Tuple innerTableTuple;
    tableMetadata->table_->GetTuple(*ridIderator, &innerTableTuple, exec_ctx_->GetTransaction());
    std::vector<Value> result;
    result.reserve(GetOutputSchema()->GetColumnCount());
    for (auto &mycolumn : GetOutputSchema()->GetColumns()) {
      Value value = mycolumn.GetExpr()->EvaluateJoin(&innerTableTuple, plan_->InnerTableSchema(), &outTableTuple,
                                                     plan_->OuterTableSchema());
      result.emplace_back(value);
    }
    ridIderator++;
    *tuple = Tuple(result, GetOutputSchema());
    return true;
  }
  while (true) {
    RID outRid;
    if (!child_executor_->Next(&outTableTuple, &outRid)) {
      return false;
    }
    // Schema indexSchema = innerIndexInfo->key_schema_;
    // 列名相等肯定是不行的
    const ColumnValueExpression *columnValueExpression =
        dynamic_cast<const ColumnValueExpression *>(plan_->Predicate()->GetChildAt(0));
    uint32_t col_idx = columnValueExpression->GetColIdx();
    Tuple keyTuple = outTableTuple.KeyFromTuple(
        *plan_->OuterTableSchema(), Schema(std::vector<Column>{plan_->OuterTableSchema()->GetColumn(col_idx)}),
        std::vector<uint32_t>{col_idx});
    result.clear();
    innerIndexInfo->index_->ScanKey(keyTuple, &result, exec_ctx_->GetTransaction());
    // 只能找到一条？
    if (!result.empty()) {
      ridIderator = result.begin();
      break;
    }
  }
  Tuple innerTableTuple;
  tableMetadata->table_->GetTuple(*ridIderator, &innerTableTuple, exec_ctx_->GetTransaction());
  std::vector<Value> result;
  result.reserve(GetOutputSchema()->GetColumnCount());
  for (auto &mycolumn : GetOutputSchema()->GetColumns()) {
    Value value = mycolumn.GetExpr()->EvaluateJoin(&innerTableTuple, plan_->InnerTableSchema(), &outTableTuple,
                                                   plan_->OuterTableSchema());
    result.emplace_back(value);
  }
  ridIderator++;
  *tuple = Tuple(result, GetOutputSchema());
  return true;
}

}  // namespace bustub
