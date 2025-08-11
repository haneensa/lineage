#include "fade/physical_caching_operator.hpp"
#include "fade/fade.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

#include <iostream>

namespace duckdb {
PhysicalCachingOperator::PhysicalCachingOperator(vector<LogicalType> types, PhysicalOperator& child,
        idx_t operator_id, idx_t query_id, PhysicalOperator& parent)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child.estimated_cardinality),
      operator_id(operator_id), query_id(query_id) {
      string qid_opid = to_string(query_id) + "_" + to_string(operator_id);
      if (parent.type == PhysicalOperatorType::HASH_GROUP_BY) {
        PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(&parent);
        InitAggInfo(qid_opid, gb->grouped_aggregate_data.aggregates, this->types);
      } else if (parent.type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
        PhysicalPerfectHashAggregate * gb = dynamic_cast<PhysicalPerfectHashAggregate *>(&parent);
        InitAggInfo(qid_opid, gb->aggregates, this->types);
      } else {
        PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(&parent);
        InitAggInfo(qid_opid, gb->aggregates, this->types);
      }

      payload_data = FadeState::payload_data[qid_opid];
      children.push_back(child);
}

class PhysicalCachingState : public OperatorState {
public:
  explicit PhysicalCachingState(ExecutionContext &context, idx_t query_id, idx_t operator_id)
    : query_id(query_id), operator_id(operator_id) {}

public:
  void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
    if (LineageState::capture == false || LineageState::persist == false) return;
    string qid_opid = to_string(query_id) + "_" + to_string(operator_id);
    if (!FadeState::cached_cols[qid_opid].empty()) return;
    std::cout << qid_opid << " CACHE " << cached_cols_sizes.size() << std::endl;
    FadeState::cached_cols[qid_opid] = std::move(cached_cols);
	  FadeState::cached_cols_sizes[qid_opid] = std::move(cached_cols_sizes);
  }
   
  unordered_map<idx_t, vector<Vector> > cached_cols;
  vector<uint32_t> cached_cols_sizes;
  idx_t query_id;
  idx_t operator_id;
  idx_t n_input;
};


unique_ptr<OperatorState> PhysicalCachingOperator::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<PhysicalCachingState>(context, query_id, operator_id);
}

OperatorResultType PhysicalCachingOperator::Execute(ExecutionContext &context,
                         DataChunk &input, 
                         DataChunk &chunk,
                         GlobalOperatorState &gstate,
                         OperatorState &state_p) const {
    auto &state = state_p.Cast<PhysicalCachingState>();
    state.n_input += input.size();
    chunk.Reference(input); // reference payload from the input
    
    if (LineageState::persist) {
      for (auto& out_var : payload_data) {
        // TODO: handle multi-threading. write to independent partitions.
        int col_idx = out_var.first;
        Vector annotations(input.data[col_idx].GetType());
        VectorOperations::Copy(input.data[col_idx], annotations, input.size(), 0, 0);
        state.cached_cols[col_idx].push_back(std::move(annotations));
      }
      state.cached_cols_sizes.push_back(input.size());
    }

    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
