//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tableBeginIderator(nullptr, RID{}, exec_ctx_->GetTransaction()),
      tableEndIderator(nullptr, RID{}, exec_ctx_->GetTransaction()) {}

void SeqScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  tableMeta = catalog->GetTable(plan_->GetTableOid());
  tableBeginIderator = tableMeta->table_->Begin(exec_ctx_->GetTransaction());
  tableEndIderator = tableMeta->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (tableBeginIderator != tableEndIderator) {
    Tuple mytuple = *tableBeginIderator;
    // predicate 可能为空
    if (plan_->GetPredicate() != nullptr) {
      tableBeginIderator++;
      if (plan_->GetPredicate()->Evaluate(&mytuple, &tableMeta->schema_).GetAs<bool>()) {
        // 构造出 需要的output schema
        const Schema *output_schema = plan_->OutputSchema();
        std::vector<Value> tmp;
        tmp.reserve(output_schema->GetColumnCount());
        for (auto &myColumn : output_schema->GetColumns()) {
          // std::string columnName = myColumn.GetName();
          // tmp.emplace_back(mytuple.GetValue(&tableMeta->schema_, tableMeta->schema_.GetColIdx(columnName)));
          const AbstractExpression *abstractExpression = myColumn.GetExpr();
          tmp.emplace_back(abstractExpression->Evaluate(&mytuple, &tableMeta->schema_));
        }
        *tuple = Tuple(tmp, output_schema);
        *rid = RID(mytuple.GetRid().Get());
        return true;
      }
      continue;
    }
    const Schema *output_schema = plan_->OutputSchema();
    std::vector<Value> tmp;
    tmp.reserve(output_schema->GetColumnCount());
    for (auto &myColumn : output_schema->GetColumns()) {
      const AbstractExpression *abstractExpression = myColumn.GetExpr();
      tmp.emplace_back(abstractExpression->Evaluate(&mytuple, &tableMeta->schema_));
    }
    *tuple = Tuple(tmp, output_schema);
    *rid = RID(mytuple.GetRid().Get());
    tableBeginIderator++;
    return true;
  }
  return false;
}

}  // namespace bustub
