#pragma once
#include "duckdb.hpp"

namespace duckdb {

// Represents a node in the logical-plan-derived lineage graph.
// Each node corresponds 1:1 with a LogicalOperator instance.
struct LineageInfoNode {
  idx_t opid;               // incremental id to each LogicalOperator assigned top-down
  
  // Logical plan structure
  vector<idx_t> children;  // opids of logical children (order preserved)
  
  // Scan metadata (only valid for LOGICAL_GET)
  string table_name = "";
  vector<string> columns;
  
  bool materializes_lineage = false;

  // Lineage graph edges
  // nearest ancestor that materializes lineage (materializes_lineage=True)
  idx_t sink_opid = DConstants::INVALID_INDEX;
  // downstream lineage sources (arity depends on operator) (materializes_lineage=True)
  vector<idx_t> source_opids;

  LogicalOperatorType type; // operator's logical type
  JoinType join_type = JoinType::INVALID;       // join type if join operator
  bool delim_flipped = false;
  idx_t n_output = DConstants::INVALID_INDEX;
  idx_t n_input = DConstants::INVALID_INDEX;

  LineageInfoNode(idx_t opid, LogicalOperatorType type) : opid(opid), type(type) {}
};

idx_t AnnotatePlan(unique_ptr<LogicalOperator> &op, idx_t query_id, idx_t& next_opid);
void PostAnnotate(idx_t query_id, idx_t root);
std::string serialize_to_json(idx_t qid, idx_t root);

} // namespace duckdb
