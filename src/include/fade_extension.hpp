#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/client_data.hpp"

#include "fade/fade_node.hpp"

namespace duckdb {

class FadeNode;

struct FadeState {
  static idx_t num_worker;
  static bool is_equal;
  static unique_ptr<FadeNode> cached_fade_result;
  static unordered_map<string, unordered_map<string, vector<int32_t>>> table_col_annotations;
  static unordered_map<string, idx_t> col_n_unique;
  static unordered_map<string, idx_t> table_count;
  static unordered_map<string, unique_ptr<MaterializedQueryResult>> codes;
  static unordered_map<string, vector<string>> cached_spec_map;
  static vector<string> cached_spec_stack;
};

unordered_map<string, vector<string>>  parse_spec(vector<string>&columns_spec);
void read_annotations(ClientContext &context, unordered_map<string, vector<string>>& spec);
void get_cached_vals(idx_t qid, idx_t opid);
void get_cached_lineage(idx_t query_id, idx_t opid);
void populate_and_verify_n_input_output(idx_t qid, idx_t opid);
void compute_count_sum_sum2(idx_t qid, idx_t root_id);
unique_ptr<MaterializedQueryResult> get_codes(ClientContext& context, string table, string col);

void AdjustOutputIds(idx_t qid, idx_t opid, vector<int>& groups);
void WhatIfSparse(ClientContext &context, int qid, int aggid,
                  unordered_map<string, vector<string>>& spec,
                  vector<int>& groups);

class FadeExtension : public Extension {
public:
   void Load(DuckDB &db) override;
   std::string Name() override;
   std::string Version() const override { return "v0.0.0"; }
};

} // namespace duckdb
