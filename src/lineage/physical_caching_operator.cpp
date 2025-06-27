#include "lineage/physical_caching_operator.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/operator/logical_join.hpp"


namespace duckdb {
PhysicalCachingOperator::PhysicalCachingOperator(vector<LogicalType> types, PhysicalOperator& child,
        idx_t operator_id, idx_t query_id)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child.estimated_cardinality),
      operator_id(operator_id), query_id(query_id) {
      children.push_back(child);
}

class PhysicalCachingState : public OperatorState {
public:
  explicit PhysicalCachingState(ExecutionContext &context, idx_t query_id, idx_t operator_id)
    : query_id(query_id), operator_id(operator_id), n_input(0) {}

public:
  void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
    if (LineageState::capture == false || LineageState::persist == false) return;
    

	  auto &agg_info = LineageState::qid_plans[query_id][operator_id]->agg_info;
    if (!agg_info->cached_cols.empty()) return;
    agg_info->cached_cols = std::move(cached_cols);
    agg_info->cached_cols_sizes = std::move(cached_cols_sizes);

    LineageState::qid_plans[query_id][operator_id]->n_input = n_input;
  }
   
  unordered_map<int, vector<Vector>> cached_cols;
  vector<int> cached_cols_sizes;
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
    // reference payload from the input
    chunk.Reference(input);
    
    if (LineageState::persist) {
	    auto &agg_info = LineageState::qid_plans[query_id][operator_id]->agg_info;
      for (auto& out_var : agg_info->payload_data) {
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
