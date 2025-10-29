#include "lineage/lineage_init.hpp"
#include "fade/fade.hpp"
#include "fade/prov_poly_eval.hpp"
#include "fade/get_predicates.hpp"
#include "fade/fade_reader.hpp"

#include "duckdb/main/extension_util.hpp"

#include <iostream>
#include <string>

namespace duckdb {

bool FadeState::debug = false;
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

unordered_map<string, std::unordered_map<string, OutPayload>> FadeState::alloc_typ_vars;

unordered_map<string, unique_ptr<MaterializedQueryResult>> FadeState::codes;
unordered_map<string, vector<string>> FadeState::cached_spec_map;
vector<string> FadeState::cached_spec_stack;

inline void PragmaWhatif(ClientContext &context, const FunctionParameters &parameters) {
  FadeState::cached_spec_stack.clear();
	int qid = parameters.values[0].GetValue<int>();
	int agg_idx = parameters.values[1].GetValue<int>();
  auto oids = ListValue::GetChildren(parameters.values[2]);
  auto spec_values = ListValue::GetChildren(parameters.values[3]);
  unordered_map<string, vector<string>> spec_map = parse_spec(spec_values);
  WhatIfSparse(context, qid, agg_idx, spec_map, oids);
  FadeState::cached_spec_map = std::move(spec_map);
}

inline void PragmaWhatifDense(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
	int agg_idx = parameters.values[1].GetValue<int>();
  auto oids = ListValue::GetChildren(parameters.values[2]);
  auto spec_values = ListValue::GetChildren(parameters.values[3]);
  unordered_map<string, vector<string>> spec_map = parse_spec(spec_values);
  if (FadeState::debug) std::cout << "****** whatifdense *******" << std::endl;
  WhatIfDense(context, qid, agg_idx, spec_map, oids);
}


// dense:
// 1) input: intervention predicate  -> target matrix
/* a) evaluate, b) construct target matrix
    for v in [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10), (10, 100000000000000000000000)]:
        l = v[0]
        r = v[1]
        res = f"""(PIName IN (select PIName from (SELECT PIName, num
                FROM (SELECT PIName, count(1) as num FROM Investigator GROUP BY PIName)
                WHERE num >= {l} and num <= {r}))) as e{n}"""
        exp_dict["Investigator"].append(res)
        n += 1
    
    for v in [(0, 1), (1, 5), (5, 10), (10, 50), (50, 100), (100, 100000000000000000000000)]:
        l = v[0]
        r = v[1]
        res = f"""(PIName IN (SELECT PIName FROM (SELECT i.PIName, sum(a.amount/1000000) as amt FROM Investigator i, Award a where i.aid=a.aid GROUP BY PIName
                    HAVING amt >= {l} and amt < {r}))) as e{n}"""
        exp_dict["Investigator"].append(res)
        n += 1
    
    for v in [(0,  1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (7, 8), (8, 9), (9, 10), (10, 11), (11, 100000000000000000000000)]:
        l = v[0]
        r = v[1]
        res = f"""(aid IN (SELECT aid FROM (SELECT aid, date_sub('year', strptime(startdate, '%m/%d/%Y'), strptime(enddate, '%m/%d/%Y')) as num_years 
                 FROM Award WHERE num_years >= {l} and num_years < {r}))) as e{n}"""

        exp_dict["Award"].append(res)
        n += 1
    M_pad = np.concatenate((prefix_extra_columns, I.to_numpy().astype(np.uint8), extra_columns), axis=1)
    packed_array = np.packbits(M_pad, axis=1)
*/
// 2) memory allocation
// 3) intervention evaluation

// 1) prepapre_lineage: query id
inline void PragmaPrepareLineage(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  idx_t root_id = LineageState::qid_plans_roots[qid];
  if (LineageState::debug) std::cout << "PRAGMA PrepapreLineage: " << qid << " " << root_id << 
  " " << EnumUtil::ToChars<LogicalOperatorType>(LineageState::qid_plans[qid][root_id]->type)
  << std::endl;
  InitGlobalLineage(qid, root_id);
}

inline void PragmaPrepareLineageBuff(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  idx_t root_id = LineageState::qid_plans_roots[qid];
  if (LineageState::debug) std::cout << "PRAGMA PrepapreLineage: " << qid << " " << root_id << 
  " " << EnumUtil::ToChars<LogicalOperatorType>(LineageState::qid_plans[qid][root_id]->type)
  << std::endl;
  InitGlobalLineageBuff(context, qid, root_id);
}

inline void PragmaPrepareFade(ClientContext &context, const FunctionParameters &parameters) {
  auto spec_values = ListValue::GetChildren(parameters.values[0]);
  if (FadeState::debug) std::cout << "PRAGMA PrepapreFade: " << spec_values.size() << std::endl;
  // 1. Parse: spec. Input (t.col1|t.col2|..)
  unordered_map<string, vector<string>> spec_map = parse_spec(spec_values);
  // 2. Read: annotations
  read_annotations(context, spec_map);
  
	int qid = 0; //parameters.values[0].GetValue<int>();
  idx_t root_id = LineageState::qid_plans_roots[qid];
  GetCachedVals(qid, root_id);
  RecomputeAggs(qid, root_id);
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
    
    auto prepare_lineagebuf_fun = PragmaFunction::PragmaCall("PrepareLineageBuff",
        PragmaPrepareLineageBuff, {LogicalType::INTEGER});
    ExtensionUtil::RegisterFunction(db_instance, prepare_lineagebuf_fun);

    auto whatif_fun = PragmaFunction::PragmaCall("Whatif",
        PragmaWhatif, {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER),
        LogicalType::LIST(LogicalType::VARCHAR)});
    ExtensionUtil::RegisterFunction(db_instance, whatif_fun);
    
    auto whatif_dense_fun = PragmaFunction::PragmaCall("WhatifDense",
        PragmaWhatifDense, {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER),
        LogicalType::LIST(LogicalType::VARCHAR)});
    ExtensionUtil::RegisterFunction(db_instance, whatif_dense_fun);
}


}
