//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_({plan_->GetAggregates(), plan_->GetAggregateTypes()}),
      aht_iterator_({}) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  try {
    Tuple mytuple;
    RID rid;
    child_->Init();
    if (!plan_->GetGroupBys().empty()) {
      while (child_->Next(&mytuple, &rid)) {
        AggregateKey group_by = MakeKey(&mytuple);
        AggregateValue aggre_value = MakeVal(&mytuple);
        aht_.InsertCombine(group_by, aggre_value);
      }
    } else {
      while (child_->Next(&mytuple, &rid)) {
        AggregateValue aggre_value = MakeVal(&mytuple);
        aht_.InsertCombine(AggregateKey{}, aggre_value);
      }
    }
  } catch (Exception &e) {
    throw "you met error";
  }

  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    AggregateValue aggre_value = aht_iterator_.Val();
    if (plan_->GetHaving() != nullptr) {
      if (plan_->GetHaving()
              ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
              .GetAs<bool>()) {
        // 把值给 tuple 还是要拼凑
        std::vector<Value> aggreVector = aht_iterator_.Val().aggregates_;
        std::vector<Value> groupbyVector = aht_iterator_.Key().group_bys_;
        std::vector<Value> result;
        result.reserve(GetOutputSchema()->GetColumnCount());
        for (auto &mycolumn : GetOutputSchema()->GetColumns()) {
          Value value = mycolumn.GetExpr()->EvaluateAggregate(groupbyVector, aggreVector);
          result.emplace_back(value);
        }
        *tuple = Tuple(result, GetOutputSchema());
        ++aht_iterator_;
        return true;
      }
      ++aht_iterator_;
      continue;
    }
    std::vector<Value> aggreVector = aht_iterator_.Val().aggregates_;
    std::vector<Value> groupbyVector = aht_iterator_.Key().group_bys_;
    std::vector<Value> result;
    result.reserve(GetOutputSchema()->GetColumnCount());
    for (auto &mycolumn : GetOutputSchema()->GetColumns()) {
      Value value = mycolumn.GetExpr()->EvaluateAggregate(groupbyVector, aggreVector);
      result.emplace_back(value);
    }
    *tuple = Tuple(result, GetOutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub
