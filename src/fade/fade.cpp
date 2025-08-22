#include "lineage/lineage_init.hpp"
#include "fade/fade.hpp"
#include "fade/prov_poly_eval.hpp"
#include "fade/get_predicates.hpp"
#include "fade/fade_reader.hpp"

#include "duckdb/main/extension_util.hpp"

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

unordered_map<string, unique_ptr<MaterializedQueryResult>> FadeState::codes;
unordered_map<string, vector<string>> FadeState::cached_spec_map;
vector<string> FadeState::cached_spec_stack;

inline void PragmaWhatif(ClientContext &context, const FunctionParameters &parameters) {
  FadeState::cached_spec_stack.clear();
	int qid = parameters.values[0].GetValue<int>();
	int agg_idx = parameters.values[1].GetValue<int>();
  auto oids = ListValue::GetChildren(parameters.values[2]);
  auto spec_values = ListValue::GetChildren(parameters.values[3]);
  std::cout << "WhatIf: " << qid << " " << agg_idx << " " << oids.size() << " " << spec_values.size() << std::endl;
  unordered_map<string, vector<string>> spec_map = parse_spec(spec_values);
  WhatIfSparse(context, qid, agg_idx, spec_map, oids);
  // TODO: make it per qid?
  FadeState::cached_spec_map = std::move(spec_map);
}

// 1) prepapre_lineage: query id
inline void PragmaPrepareLineage(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  idx_t root_id = LineageState::qid_plans_roots[qid];
  std::cout << "PRAGMA PrepapreLineage: " << qid << " " << root_id << std::endl;
  InitGlobalLineage(qid, root_id);
  GetCachedVals(qid, root_id);
}

inline void PragmaPrepareFade(ClientContext &context, const FunctionParameters &parameters) {
  auto spec_values = ListValue::GetChildren(parameters.values[0]);
  std::cout << "PRAGMA PrepapreFade: " << spec_values.size() << std::endl;
  // 1. Parse: spec. Input (t.col1|t.col2|..)
  unordered_map<string, vector<string>> spec_map = parse_spec(spec_values);
  // 2. Read: annotations
  read_annotations(context, spec_map);
}


void InitFuncs(DatabaseInstance& db_instance) {
    TableFunction pe_func("PolyEval", {LogicalType::INTEGER, LogicalType::VARCHAR},
        PolyEvalFunction::PolyEvalImplementation,
        PolyEvalFunction::PolyEvalBind, PolyEvalFunction::PolyEvalInit);
    ExtensionUtil::RegisterFunction(db_instance, pe_func);

    // qid, agg id
    TableFunction fade_reader("fade_reader", {LogicalType::INTEGER, LogicalType::INTEGER},
        FadeReaderFunction::FadeReaderImplementation, FadeReaderFunction::FadeReaderBind,
        FadeReaderFunction::FadeReaderInitGlobal, FadeReaderFunction::FadeReaderInitLocal);
    ExtensionUtil::RegisterFunction(db_instance, fade_reader);
    
    TableFunction fade_predicates("GetPredicates", {LogicalType::INTEGER},
        GetPredicatesFunction::GetPredicatesImpl, GetPredicatesFunction::GetPredicatesBind,
        nullptr,
        GetPredicatesFunction::GetPredicatesInitLocal);
    ExtensionUtil::RegisterFunction(db_instance, fade_predicates);
    
    auto prepare_fade_fun = PragmaFunction::PragmaCall("PrepareFade",
        PragmaPrepareFade, {LogicalType::LIST(LogicalType::VARCHAR)});
    ExtensionUtil::RegisterFunction(db_instance, prepare_fade_fun);

    auto prepare_lineage_fun = PragmaFunction::PragmaCall("PrepareLineage",
        PragmaPrepareLineage, {LogicalType::INTEGER});
    ExtensionUtil::RegisterFunction(db_instance, prepare_lineage_fun);

    auto whatif_fun = PragmaFunction::PragmaCall("Whatif",
        PragmaWhatif, {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER),
        LogicalType::LIST(LogicalType::VARCHAR)});
    ExtensionUtil::RegisterFunction(db_instance, whatif_fun);
}


}
