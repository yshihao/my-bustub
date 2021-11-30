//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  // 这个类的 成员executor 最好在初始化就做好
  left_executor_->Init();
  right_executor_->Init();
  Tuple childTuple;
  RID rid;
  while (left_executor_->Next(&childTuple, &rid)) {
    left_tuples.emplace_back(childTuple);
  }
  while (right_executor_->Next(&childTuple, &rid)) {
    right_tuples.emplace_back(childTuple);
  }
  leftIterator = left_tuples.begin();
  rightIterator = right_tuples.begin();
  // LOG_INFO("left child size %lu\n", left_tuples.size());
  // LOG_INFO("right child size %lu\n", right_tuples.size());
}
std::vector<Value> NestedLoopJoinExecutor::GetResultTuple(const Tuple &leftTuple, const Tuple &rightTuple) {
  std::vector<Value> result{};
  const Schema *output_schema = plan_->OutputSchema();
  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();
  result.reserve(output_schema->GetColumnCount());
  for (auto &myColumn : output_schema->GetColumns()) {
    const AbstractExpression *abstractExpression = myColumn.GetExpr();
    result.emplace_back(abstractExpression->EvaluateJoin(&leftTuple, left_schema, &rightTuple, right_schema));
  }
  // for (auto &myColumn : output_schema->GetColumns()) {
  //   std::string columnName = myColumn.GetName();
  //   bool flag = true;
  //   for (uint32_t i = 0; i < left_schema->GetColumnCount(); i++) {
  //     if (left_schema->GetColumn(i).GetName() == myColumn.GetName()) {
  //       flag = false;
  //       result.emplace_back(leftTuple.GetValue(left_schema, i));
  //       break;
  //     }
  //   }
  //   if (!flag) {
  //     continue;
  //   }
  //   for (uint32_t i = 0; i < right_schema->GetColumnCount(); i++) {
  //     if (right_schema->GetColumn(i).GetName() == myColumn.GetName()) {
  //       result.emplace_back(rightTuple.GetValue(right_schema, i));
  //       break;
  //     }
  //   }
  // }
  return result;
}
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  for (; leftIterator < left_tuples.end(); ++leftIterator) {
    Tuple leftTuple = *leftIterator;
    // if (rightIterator == right_tuples.end()) {
    //   rightIterator = right_tuples.begin();
    // }
    for (; rightIterator < right_tuples.end(); ++rightIterator) {
      Tuple rightTuple = *rightIterator;
      if (plan_->Predicate() != nullptr) {
        if (plan_->Predicate()
                ->EvaluateJoin(&leftTuple, left_executor_->GetOutputSchema(), &rightTuple,
                               right_executor_->GetOutputSchema())
                .GetAs<bool>()) {
          std::vector<Value> result = GetResultTuple(leftTuple, rightTuple);
          *tuple = Tuple(result, plan_->OutputSchema());
          ++rightIterator;
          // 因为直接返回 若到了最后一个元素 要++leftIterator 和 重置rightIterator
          if (rightIterator == right_tuples.end()) {
            ++leftIterator;
            rightIterator = right_tuples.begin();
          }
          return true;
        }
        continue;
      }
      std::vector<Value> result = GetResultTuple(leftTuple, rightTuple);
      *tuple = Tuple(result, plan_->OutputSchema());
      ++rightIterator;
      if (rightIterator == right_tuples.end()) {
        ++leftIterator;
        rightIterator = right_tuples.begin();
      }
      return true;
    }
    // 注意 正常结束后 需要重置。否则rightIterator会一直处于 end()
    rightIterator = right_tuples.begin();
  }
  return false;

  // std::vector<Column> cols;
  // uint32_t left_elements = left_schema->GetColumnCount();
  // uint32_t right_elements = right_schema->GetColumnCount();
  // cols.reserve(left_elements + right_elements);
  // for (int i = 0; i < left_elements; i++) {
  //   cols.emplace_back(left_schema->GetColumn(i));
  // }
  // for (int i = 0; i < right_elements; i++) {
  //   cols.emplace_back(right_schema->GetColumn(i));
  // }
  // // 分配空间处理的问题
  // Schema *resultSchema = new Schema(cols);
}

}  // namespace bustub
