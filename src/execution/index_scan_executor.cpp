//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      indexBeginIterator(IndexIterator<GenericKey<8>, RID, GenericComparator<8>>({}, {})),
      indexEndIterator(IndexIterator<GenericKey<8>, RID, GenericComparator<8>>({}, {})) {}

void IndexScanExecutor::Init() {
  IndexInfo *indexInfo = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tableMeta = exec_ctx_->GetCatalog()->GetTable(indexInfo->table_name_);
  BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *bplusTreeIndex =
      dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(indexInfo->index_.get());
  indexBeginIterator = bplusTreeIndex->GetBeginIterator();
  indexEndIterator = bplusTreeIndex->GetEndIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (indexBeginIterator != indexEndIterator) {
    std::pair<GenericKey<8>, RID> mappingType = *indexBeginIterator;
    Tuple mytuple;
    bool result = tableMeta->table_->GetTuple(mappingType.second, &mytuple, exec_ctx_->GetTransaction());
    if (!result) {
      return false;
    }
    if (plan_->GetPredicate()->Evaluate(&mytuple, &tableMeta->schema_).GetAs<bool>()) {
      const Schema *output_schema = plan_->OutputSchema();
      std::vector<Value> tmp;
      tmp.reserve(output_schema->GetColumnCount());
      for (auto &myColumn : output_schema->GetColumns()) {
        std::string columnName = myColumn.GetName();
        tmp.emplace_back(mytuple.GetValue(&tableMeta->schema_, tableMeta->schema_.GetColIdx(columnName)));
      }
      *tuple = Tuple(tmp, GetOutputSchema());
      *rid = RID(mappingType.second.Get());
      ++indexBeginIterator;
      return true;
    }
    ++indexBeginIterator;
  }
  return false;
}

}  // namespace bustub
