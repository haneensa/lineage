#include "lineage/physical_lineage_operator.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/operator/logical_join.hpp"


namespace duckdb {
PhysicalLineageOperator::PhysicalLineageOperator(vector<LogicalType> types, PhysicalOperator& child,
        idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type, int source_count,
        idx_t left_rid, idx_t right_rid, bool is_root, string join_type)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child.estimated_cardinality),
      is_root(is_root), dependent_type(dependent_type), source_count(source_count),
      operator_id(operator_id), query_id(query_id),
      left_rid(left_rid), right_rid(right_rid), join_type(join_type) {
      children.push_back(child);
}

class PhysicalLineageState : public OperatorState {
public:
  explicit PhysicalLineageState(ExecutionContext &context, idx_t query_id, idx_t operator_id,
      LogicalOperatorType dependent_type, int source_count, string join_type, idx_t partition_id) 
    : offset(0), n_input(0), query_id(query_id), operator_id(operator_id), source_count(source_count),
      join_type(join_type), dependent_type(dependent_type), partition_id(partition_id) {
  }

public:
  void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
    if (LineageState::capture == false || LineageState::persist == false) return;
    
    string table_name = to_string(query_id) + "_" + to_string(operator_id); // + "_" + to_string(partition_id);

    //if (LineageState::debug) {
      std::cout << "[DEBUG] <persist lineage> partition_id" << partition_id << " "
        << "qid_opid:" <<  table_name  << ", left len: " << lineage.size()
        << ", right len: " << lineage_right.size() << ", type: "
        << EnumUtil::ToChars<LogicalOperatorType>(this->dependent_type) << std::endl;
  //  }

    if (LineageState::lineage_store[table_name].size()) return;
    LineageState::lineage_store[table_name] = std::move(lineage);
    if (source_count == 2 || join_type=="RIGHT_SEMI" || join_type=="RIGHT") {
      LineageState::lineage_store[table_name+"_right"] = std::move(lineage_right);
    }
    //LineageState::qid_plans[query_id][operator_id]->n_output = n_input;
  }
   
  // todo: maybe decompose it into two arrays? one for Vectors one for size
  // this way we don't have to duplicate size for the right side
  vector<std::pair<Vector, int>> lineage;
  vector<std::pair<Vector, int>> lineage_right;
  LogicalOperatorType dependent_type;
  idx_t query_id;
  idx_t operator_id;
  idx_t offset;
  idx_t n_input;
  idx_t partition_id;
  int source_count;
  string join_type;
};


unique_ptr<OperatorState> PhysicalLineageOperator::GetOperatorState(ExecutionContext &context) const {
  // set the partition idx for the first decendant lineage operator, then use the same index
  lock_guard<mutex> lock(LineageState::glock);
  string table_name = to_string(query_id) + "_" + to_string(operator_id);
  idx_t partition_idx = LineageState::partitions[table_name]++;
	return make_uniq<PhysicalLineageState>(context, query_id, operator_id, dependent_type,
      source_count, join_type, partition_idx);
}

OperatorResultType PhysicalLineageOperator::Execute(ExecutionContext &context,
                         DataChunk &input, 
                         DataChunk &chunk,
                         GlobalOperatorState &gstate_p,
                         OperatorState &state_p) const {
    auto &state = state_p.Cast<PhysicalLineageState>();
    state.n_input += input.size();
    if (LineageState::debug) {
      std::cout << " [PhysicalLineageOperator] opid: " << operator_id << "len(input): " << input.size() << " join_type:" <<  join_type << ", source_count: " << source_count 
      << ", left_rid: " << left_rid << ", right_rid:" << right_rid << ", dependent_type: " << 
       EnumUtil::ToChars<LogicalOperatorType>(this->dependent_type) << std::endl;
      std::cout << input.ColumnCount() << std::endl;
      for (auto &type : input.GetTypes()) { std::cout << type.ToString() << " "; }
      std::cout << " ---> ";
      for (auto &type : chunk.GetTypes()) { std::cout << type.ToString() << " "; }
      std::cout << "\n -------" << std::endl;
    }
    if (left_rid == 0 && right_rid > 0) { // right semi join
      chunk.SetCardinality(input);
      chunk.Reference(input);
      // pass annotations to parent since it is single annotations
      return OperatorResultType::NEED_MORE_INPUT;
    }

    if (this->dependent_type == LogicalOperatorType::LOGICAL_CHUNK_GET) { 
      chunk.SetCapacity(input);
      chunk.SetCardinality(input);
      for (idx_t i = 0; i < left_rid; i++) {
        chunk.data[i].Reference(input.data[i]);
      }
      // Append row identifier since it's hard to modify Chunk Get
      chunk.data.back().Sequence(state.offset, 1, input.size());
      state.offset += input.size();
      return OperatorResultType::NEED_MORE_INPUT;
    }

    // reference payload from the input
    chunk.SetCapacity(input);
    chunk.SetCardinality(input);
    for (idx_t i = 0; i < left_rid; i++) {
      chunk.data[i].Reference(input.data[i]);
    }
    
    if (join_type == "MARK") {
      // pass annotations to parent since it is single annotations
      chunk.data.back().Reference(input.data[left_rid]);
      chunk.data[left_rid].Reference(input.data.back());
      return OperatorResultType::NEED_MORE_INPUT;
    }
    
    for (idx_t i = left_rid+1; i < left_rid+right_rid+1; i++) {
      chunk.data[i-1].Reference(input.data[i]);
    }

    // Extract annotations payload from left input
    if (left_rid > 0 && LineageState::persist) {
      idx_t annotation_col = left_rid;
      Vector annotations(input.data[annotation_col].GetType());
      VectorOperations::Copy(input.data[annotation_col], annotations, input.size(), 0, 0);
      state.lineage.push_back({annotations, input.size()});
    }

    if (this->source_count == 2 && LineageState::persist) {
      // Extract annotations payload from the right input
      idx_t annotation_col = input.ColumnCount() - 1;
      Vector annotations(input.data[annotation_col].GetType());
      VectorOperations::Copy(input.data[annotation_col], annotations, input.size(), 0, 0);
      state.lineage_right.push_back({annotations, input.size()});
    }

    if (!is_root) {
      // This is not the root, reindex complex annotations
      chunk.data.back().Sequence(state.offset, 1, input.size());
      state.offset += input.size();
    }
    
    
    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
