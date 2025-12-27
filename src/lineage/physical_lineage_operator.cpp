#include "lineage/physical_lineage_operator.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include <cmath>
#include <atomic>



namespace duckdb {
PhysicalLineageOperator::PhysicalLineageOperator(vector<LogicalType> types, PhysicalOperator& child,
        idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type, int source_count,
        idx_t left_rid, idx_t right_rid, bool is_root, string join_type, bool pre, bool post)
      : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), child.estimated_cardinality),
      is_root(is_root), dependent_type(dependent_type), source_count(source_count),
      operator_id(operator_id), query_id(query_id),
      left_rid(left_rid), right_rid(right_rid), join_type(join_type), pre(pre), post(post) {
      if (LineageState::debug) {
        std::cout << "[DEBUG] PhysicalLineageOperator " << std::endl;
        std::cout << child.ToString() << std::endl;
      }

      children.push_back(child);
}

class LineageGlobalState : public GlobalOperatorState {
  public:
    LineageGlobalState(string table_name) : cur_partition(0), global_offset(0) {
      std::lock_guard<std::mutex> g(LineageState::g_log_lock);
      auto &ptr = LineageState::partitioned_store_buf[table_name];
      if (!ptr) {
          ptr = make_uniq<PartitionedLineage>();
      }
      partitions = ptr.get();
    }
    
    std::atomic<idx_t> cur_partition;
    std::atomic<idx_t> global_offset;
    PartitionedLineage* partitions;
};

class PhysicalLineageState : public OperatorState {
public:
  explicit PhysicalLineageState(ExecutionContext &context, idx_t query_id, idx_t operator_id,
      LogicalOperatorType dependent_type, int source_count, string join_type, bool pre, bool post)
    : n_input(0), query_id(query_id), operator_id(operator_id), source_count(source_count),
      join_type(join_type), dependent_type(dependent_type), pre(pre), post(post) {
  }

public:
  void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
    if (post || LineageState::capture == false || LineageState::persist == false) return;
    
    string table_name = to_string(query_id) + "_" + to_string(operator_id);
    if (LineageState::debug) {
      std::cout << "[DEBUG] <persist lineage> " << "qid_opid:" <<  table_name  << ", left len: " << lineage.size()
        << ", right len: " << lineage_right.size() << ", type: "
        << EnumUtil::ToChars<LogicalOperatorType>(this->dependent_type) << std::endl;
   }

    auto &gstate = op.op_state->Cast<LineageGlobalState>();
    std::lock_guard<std::mutex> g(gstate.partitions->p_lock);
    gstate.partitions->zones.push_back(std::move(zones));
    gstate.partitions->left.push_back(std::move(lineage_buffer));
    gstate.partitions->local_offsets.push_back(std::move(local_offsets));
    if (source_count == 2 || join_type=="RIGHT_SEMI" || join_type=="RIGHT") {
      gstate.partitions->right.push_back(std::move(lineage_right_buffer));
    }
  }
   
  vector<std::pair<Vector, int>> lineage;
  vector<std::pair<Vector, int>> lineage_right;
  vector<IVector> lineage_buffer;
  vector<IVector> lineage_right_buffer;
  LogicalOperatorType dependent_type;
  idx_t query_id;
  idx_t operator_id;
  idx_t n_input;
  int source_count;
  string join_type;
  bool pre, post;
  vector<idx_t> zones;
  vector<idx_t> local_offsets;
};


unique_ptr<GlobalOperatorState> PhysicalLineageOperator::GetGlobalOperatorState(ClientContext &context) const {

  string table_name = to_string(query_id) + "_" + to_string(operator_id);
  return make_uniq<LineageGlobalState>(table_name);
}

unique_ptr<OperatorState> PhysicalLineageOperator::GetOperatorState(ExecutionContext &context) const {
  return make_uniq<PhysicalLineageState>(context, query_id, operator_id, dependent_type,
      source_count, join_type, pre, post);
}

OperatorResultType PhysicalLineageOperator::Execute(ExecutionContext &context,
                         DataChunk &input, 
                         DataChunk &chunk,
                         GlobalOperatorState &gstate_p,
                         OperatorState &state_p) const {
    auto &state = state_p.Cast<PhysicalLineageState>();
    auto &gstate = gstate_p.Cast<LineageGlobalState>();
    idx_t count = input.size();
    state.n_input += input.size();
    if (LineageState::debug) {
      std::cout << " [PhysicalLineageOperator] opid: " << operator_id << "len(input): " << input.size() << " join_type:" <<  join_type << ", source_count: " << source_count 
      << ", left_rid: " << left_rid << ", right_rid:" << right_rid << ", dependent_type: " << 
       EnumUtil::ToChars<LogicalOperatorType>(this->dependent_type) << std::endl;
      std::cout << input.ColumnCount() << " pre: " << pre << " post: " << post << std::endl;
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
      idx_t g_offset = gstate.global_offset.fetch_add(count);
      chunk.data.back().Sequence(g_offset, 1, input.size());
      state.zones.emplace_back(g_offset);
      state.local_offsets.emplace_back(count);
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
    if (!post && left_rid > 0 && LineageState::persist) {
      idx_t annotation_col = left_rid;
      state.lineage_buffer.emplace_back();
      auto& entry = state.lineage_buffer.back();
      auto &vec = input.data[annotation_col];
      if (this->dependent_type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
          vec.GetType().id() == LogicalTypeId::LIST) {
        assert(ListType::GetChildType(vec.GetType()).id() == LogicalTypeId::BIGINT);
        LogListBigIntVector(vec, input.size(), entry);
      } else {
        LogVector(vec, input.size(), entry);
      }
      state.local_offsets.emplace_back(count);
    }

    if (!post && this->source_count == 2 && LineageState::persist) {
      // Extract annotations payload from the right input
      idx_t annotation_col = input.ColumnCount() - 1;
      state.lineage_right_buffer.emplace_back();
      auto& entry = state.lineage_right_buffer.back();
      LogVector(input.data[annotation_col], input.size(), entry);
    }

    if (!is_root && !pre) {
      // This is not the root, reindex complex annotations
      idx_t g_offset = gstate.global_offset.fetch_add(count);
      chunk.data.back().Sequence(g_offset, 1, count);
      // local zone map -> [g_offset, g_offset+count]
      state.zones.emplace_back(g_offset);
    }

    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
