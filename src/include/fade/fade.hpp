#pragma once

#include "lineage/lineage_init.hpp"

#include "duckdb.hpp"
#include <immintrin.h>

using Mask16 = __mmask16; //uint16_t; // __mmask16

namespace duckdb {

typedef  pair<LogicalType, vector<void*>> OutPayload;

struct FadeNode {
  idx_t n_interventions;
  idx_t n_input;
  idx_t n_output;
  idx_t num_worker;
  vector<idx_t> annotations;    // sparse
	Mask16* target_matrix; // dense
	std::unordered_map<string, OutPayload> alloc_typ_vars;
};

struct FadeResult {
  idx_t opid;
  idx_t n_output;
  idx_t n_interventions;
  vector<Value> oids;
	std::unordered_map<string, OutPayload> alloc_typ_vars;
};

struct AggFuncContext {
  string name;
  vector<string> sub_aggs;
  LogicalType return_type;

  AggFuncContext(string name, LogicalType ret_typ, vector<string> sub_aggs)
    : name(name), return_type(ret_typ), sub_aggs(std::move(sub_aggs)) {}
};

struct SubAggsContext {
  string name;
  idx_t payload_idx;
  LogicalType return_type;
  idx_t parent_idx;
  SubAggsContext(string name, LogicalType return_type, idx_t payload_idx, idx_t parent_idx) :
    name(name), return_type(return_type), payload_idx(payload_idx), parent_idx(parent_idx) {}
};

void reorder_between_root_and_agg(idx_t qid, idx_t opid, vector<Value>& oids);
pair<int, int> get_start_end(int row_count, int thread_id, int num_worker);
int get_output_opid(int query_id, idx_t operator_id);


/* helpers to init state for fade */

void InitFuncs(DatabaseInstance& db_instance);

// construct global 1D/2D per-op lineage
// set |input| and |output| for each operators
idx_t InitGlobalLineage(idx_t qid, idx_t opid);
void GetCachedVals(idx_t qid, idx_t opid);

idx_t PrepareAggsNodes(idx_t qid, idx_t opid, idx_t agg_idx,
                      unordered_map<idx_t, FadeNode>& fade_data);

idx_t PrepareSparseFade(idx_t qid, idx_t opid, idx_t agg_idx,
                      unordered_map<idx_t, FadeNode>& fade_data,
                      unordered_map<string, vector<string>>& spec_map);

// iterate over referenced aggregations by a query
// extract types, payload idx, etc;
void InitAggInfo(string qid_opid, vector<unique_ptr<Expression>>& aggs,
                vector<LogicalType>& payload_types);

template<class T>
void allocate_agg_output(FadeNode& fnode, idx_t t, idx_t n_interventions,
    int n_output, string out_var);

// dense helpers
void WhatIfDense(ClientContext& context, int qid, int aggid,
                 unordered_map<string, vector<string>>& spec_map,
                 vector<Value>& oids);

// TODO: make a plan for specializations (code gen)

// sparse helpers
unordered_map<string, vector<string>>  parse_spec(vector<Value>& cols_spec);
unique_ptr<MaterializedQueryResult> get_codes(ClientContext &context, string table, string col);
string generate_dict_query(const string &table, const vector<string> &columns);
string generate_n_unique_count(const string &table, const vector<string> &columns);
void read_annotations(ClientContext &context, unordered_map<string, vector<string>>& spec);
void WhatIfSparse(ClientContext &context, int qid, int aggid, 
                  unordered_map<string, vector<string>>& spec,
                  vector<Value>& oids);
void sparse_incremental_bw(string func, const int g, const vector<idx_t>& bw_lineage, const vector<idx_t>&var_0,
                          void* __restrict__  out,
                          unordered_map<int, pair<LogicalType, void*>>& input_data_map,
                          int n_interventions, int col_idx);

void RecomputeAggs(idx_t qid, idx_t opid);

// to evaluate all, iterate over aggs
// else, aggs[offset]
struct FadeState {
  static bool debug;
  static unordered_map<QID, FadeResult> fade_results;
  // qid_opid -> map<sub_id, fun_name> aggregations are decomposed into 0-n functions
  static unordered_map<QID_OPID, unordered_map<string, shared_ptr<SubAggsContext>>> sub_aggs;
  // qid_opid -> map<agg_id, AggContext>  contains the agg name, the sub functions ids
  static unordered_map<QID_OPID, unordered_map<idx_t, shared_ptr<AggFuncContext>>> aggs;

  static unordered_map<string, idx_t> table_count;

  // sparse annoatations
  static unordered_map<string, unordered_map<string, vector<int32_t>>> table_col_annotations;
  static unordered_map<string, idx_t> col_n_unique;
   
  // physical_cahing_operator
  static unordered_map<QID_OPID, vector<pair<idx_t, LogicalType>>> payload_data;
  static unordered_map<QID_OPID, Payload> cached_cols;
  static unordered_map<QID_OPID, vector<uint32_t>> cached_cols_sizes;
  static unordered_map<QID_OPID, unordered_map<int, pair<LogicalType, void*>>> input_data_map;
  
  static unordered_map<string, unique_ptr<MaterializedQueryResult>> codes;
  static unordered_map<string, vector<string>> cached_spec_map;
  static vector<string> cached_spec_stack;
	
  static unordered_map<string, std::unordered_map<string, OutPayload>> alloc_typ_vars;
};

} // namespace duckdb
