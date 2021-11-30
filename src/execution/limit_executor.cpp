//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  tupleReturn = 0;
  size_t i = 0;
  while (i < plan_->GetOffset()) {
    child_executor_->Next({}, {});
    i++;
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (tupleReturn >= plan_->GetLimit() || !child_executor_->Next(tuple, rid)) {
    return false;
  }
  tupleReturn++;
  return true;
}

}  // namespace bustub
