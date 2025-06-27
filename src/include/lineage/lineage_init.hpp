#pragma once
#include "duckdb.hpp"

namespace duckdb {

// TODO: write custom function extraction from Expression
// TODO: write custom function evaluation
// TODO: write custom function output reading
// TODO: write custom function global vals

//  static CreateContext(Expression) -> extract types, payload idx, etc; create SubAggs
//
struct AggFuncContext {
  string name;
  vector<string> sub_aggs;
  LogicalType return_type;

  vector<int> groups_count;
  vector<float> groups_sum;
  vector<float> groups_sum_2;

  AggFuncContext(string name, LogicalType ret_typ, vector<string> sub_aggs)
    : name(name), return_type(ret_typ), sub_aggs(sub_aggs) {}
};

// class AggContext:
//  string agg_key;
//  string fname;
//  vector<idx_t> payload_idx_vec; // avg, stddev both sum and count uses the same idx
//  LogicalType return_type;
//  vector<sting> sub_aggs; // use agg_key = fname + agg_idx
//
// sub class AggsContext
// SumFunc:
//  incremental_bw();
//  incremental_forward();
//  ReadOutput();
//  has groups_sum
//
// CountFunc:
//  incremental_bw();
//  incremental_forward();
//  ReadOutput();
//  has groups_count
//
// AvgFunc:
//  ReadOutput(); // access its subfuncs
//  has groups_sum and groups_count
//
// stddevFunc:
//  ReadOutput(); // access its subfuncs
//  hash groups_sum, groups_count, groups_sum_2

//  payload is allocated and populated independently alloc_vars too
struct SubAggsContext {
  string name;
  idx_t payload_idx;
  LogicalType return_type;
  idx_t parent_idx;
  SubAggsContext(string name, LogicalType return_type, idx_t payload_idx, idx_t parent_idx) :
    name(name), return_type(return_type), payload_idx(payload_idx), parent_idx(parent_idx) {}
};

struct AggInfo {
  // map<sub_id, fun_name> aggregations are decomposed into 0-n functions
  unordered_map<string, unique_ptr<SubAggsContext>> sub_aggs;
  // map<agg_id, AggContext>  contains the agg name, the sub functions ids
  unordered_map<idx_t, unique_ptr<AggFuncContext>> aggs;
  vector<pair<idx_t, LogicalType>> payload_data;

  int child_agg_id;
  bool has_agg_child;
  idx_t n_groups_attr;
	
  std::unordered_map<int, void*> input_data_map; // input to the aggregates
  unordered_map<int, LogicalType> input_data_types_map;
  
  unordered_map<int, vector<Vector>> cached_cols;
  vector<int> cached_cols_sizes;
};

// tree of lineage points (ignore pipelined operators because we pipeline their lineage)
struct LineageInfoNode {
  idx_t opid;
  LogicalOperatorType type;
  vector<idx_t> children;
  int n_output;
  int n_input;
  string table_name;
  vector<string> columns;
  unique_ptr<AggInfo> agg_info;
  vector<int> lineage1D;
  vector<vector<int>> lineage2D;
  LineageInfoNode(idx_t opid, LogicalOperatorType type) : opid(opid), type(type),
  n_output(0), n_input(-1) {}
};

struct LineageState {
   static idx_t query_id;
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
