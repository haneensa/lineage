#pragma once
#include "duckdb.hpp"

namespace duckdb {

// tree of lineage points (ignore pipelined operators because we pipeline their lineage)
struct LineageInfoNode {
  idx_t opid;
  int sink_id; // the first ancestor with has_lineage set or NULL if root
  vector<int> source_id; // the first child with has_lineage (extension) or leaf node
  LogicalOperatorType type;
  JoinType join_type;
  vector<idx_t> children;
  int n_output;
  int n_input;
  string table_name;
  vector<string> columns;
  bool has_lineage;
  bool delim_flipped;
  LineageInfoNode(idx_t opid, LogicalOperatorType type) : opid(opid), type(type),
  n_output(0), n_input(-1), sink_id(-1), table_name(""),
  has_lineage(false), delim_flipped(false) {}
};

struct LineageState {
   static bool capture;
   static bool persist;
   static bool debug;
   static std::unordered_map<string, LogicalOperatorType> lineage_types;
   static std::unordered_map<string, vector<std::pair<Vector, int>>> lineage_store;
   static std::unordered_map<idx_t, unordered_map<idx_t, unique_ptr<LineageInfoNode>>> qid_plans;
   static std::unordered_map<idx_t, idx_t> qid_plans_roots;
};

unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                      unique_ptr<LogicalOperator>& plan);

bool IsSPJUA(unique_ptr<LogicalOperator>& plan);

} // namespace duckdb
