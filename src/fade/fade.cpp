#include "lineage/lineage_init.hpp"
#include "fade/fade.hpp"

#include <iostream>
#include <string>

namespace duckdb {

bool FadeState::debug;
unordered_map<QID, FadeResult> FadeState::fade_results;
unordered_map<QID_OPID, unordered_map<string, shared_ptr<SubAggsContext>>> FadeState::sub_aggs;
unordered_map<QID_OPID, unordered_map<idx_t, shared_ptr<AggFuncContext>>> FadeState::aggs;
unordered_map<string, idx_t> FadeState::table_count;

// cached payload
unordered_map<QID_OPID, vector<pair<idx_t, LogicalType>>> FadeState::payload_data;
unordered_map<QID_OPID, Payload> FadeState::cached_cols;
unordered_map<QID_OPID, vector<uint32_t>> FadeState::cached_cols_sizes;
unordered_map<QID_OPID, unordered_map<int, pair<LogicalType, void*>>> FadeState::input_data_map; 


// only for sparse
unordered_map<string, unordered_map<string, vector<int32_t>>> FadeState::table_col_annotations;
unordered_map<string, idx_t> FadeState::col_n_unique;

}
