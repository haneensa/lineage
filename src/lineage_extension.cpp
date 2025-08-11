#define DUCKDB_EXTENSION_MAIN
#include "lineage_extension.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"
#include "lineage/lineage_meta.hpp"
#include "lineage/lineage_reader.hpp"
#include "lineage/lineage_global.hpp"
#include "lineage/lineage_query.hpp"

#include "fade/prov_poly_eval.hpp"
#include "fade/fade_reader.hpp"
#include "fade/fade.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

bool LineageState::capture = false;
bool LineageState::debug = false;
bool LineageState::persist = true;
std::unordered_map<string, vector<std::pair<Vector, int>>> LineageState::lineage_store;
std::unordered_map<string, vector<vector<idx_t>>> LineageState::lineage_global_store;
std::unordered_map<string, LogicalOperatorType> LineageState::lineage_types;
std::unordered_map<idx_t, unordered_map<idx_t, unique_ptr<LineageInfoNode>>> LineageState::qid_plans;
std::unordered_map<idx_t, idx_t> LineageState::qid_plans_roots;


std::string LineageExtension::Name() {
    return "lineage";
}

inline void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::lineage_store.clear();
  LineageState::lineage_types.clear();
  LineageState::qid_plans_roots.clear();
  LineageState::qid_plans.clear();

  FadeState::aggs.clear();
  FadeState::sub_aggs.clear();
  FadeState::payload_data.clear();
  FadeState::cached_cols.clear();
  FadeState::cached_cols_sizes.clear();
  FadeState::table_col_annotations.clear();
  FadeState::col_n_unique.clear();
  FadeState::table_count.clear();
  
  for (auto& m : FadeState::input_data_map) {
    for (auto& payload : m.second) {
      if (payload.second.first == LogicalType::INTEGER) {
        delete static_cast<int*>(payload.second.second);
      } else {
        delete static_cast<float*>(payload.second.second);
      }
    }
  }
  FadeState::input_data_map.clear();
}

inline void PragmaLineageDebug(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::debug  = parameters.values[0].GetValue<bool>();
}

inline void PragmaSetPersistLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::persist  = parameters.values[0].GetValue<bool>();
}

inline void PragmaSetLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::capture  = parameters.values[0].GetValue<bool>();
}

// TODO: define in fade.cpp
// 1) prepapre_lineage: query id
inline void PragmaPrepareLineage(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  idx_t last_qid = LineageState::qid_plans_roots.size()-1;
  idx_t root_id = LineageState::qid_plans_roots[last_qid];
  std::cout << "PRAGMA PrepapreLineage: " <<  last_qid << " " << root_id << std::endl;
  InitGlobalLineage(last_qid, root_id);
  GetCachedVals(last_qid, root_id);
  //compute_count_sum_sum2(qid, root_id);
}

inline void PragmaPrepareFade(ClientContext &context, const FunctionParameters &parameters) {
  auto spec_values = ListValue::GetChildren(parameters.values[0]);
  std::cout << "PRAGMA PrepapreFade: " << spec_values.size() << std::endl;
  // 1. Parse: spec. Input (t.col1|t.col2|..)
  unordered_map<string, vector<string>> spec_map = parse_spec(spec_values);
  // 2. Read: annotations
  read_annotations(context, spec_map);
}

inline void PragmaWhatif(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  idx_t last_qid = LineageState::qid_plans_roots.size()-1;
	int agg_idx = parameters.values[1].GetValue<int>();
  auto oids = ListValue::GetChildren(parameters.values[2]);
  auto spec_values = ListValue::GetChildren(parameters.values[3]);
  std::cout << "WhatIf: " << last_qid << " " << agg_idx << " " << oids.size() << " " << spec_values.size() << std::endl;
  unordered_map<string, vector<string>> spec_map = parse_spec(spec_values);
  WhatIfSparse(context, last_qid, agg_idx, spec_map, oids);
}


void LineageExtension::Load(DuckDB &db) {
    auto optimizer_extension = make_uniq<OptimizerExtension>();
    optimizer_extension->optimize_function = [](OptimizerExtensionInput &input, 
                                            unique_ptr<LogicalOperator> &plan) {
      if (IsSPJUA(plan) == false || LineageState::capture == false) return;
      if (LineageState::debug)
        std::cout << "Plan prior to modifications: \n" << plan->ToString() << std::endl;
      plan = AddLineage(input, plan);
      //if (LineageState::debug)
        std::cout << "Plan after to modifications: \n" << plan->ToString() << std::endl;
    };

    auto &db_instance = *db.instance;
    db_instance.config.optimizer_extensions.emplace_back(*optimizer_extension);
    
  	ExtensionUtil::RegisterFunction(db_instance, LineageScanFunction::GetFunctionSet());
    TableFunction pragma_func("pragma_latest_qid", {}, LineageMetaFunction::LineageMetaImplementation, 
        LineageMetaFunction::LineageMetaBind);
    ExtensionUtil::RegisterFunction(db_instance, pragma_func);

    auto debug_fun = PragmaFunction::PragmaCall("set_debug_lineage", PragmaLineageDebug, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, debug_fun);

    auto clear_lineage_fun = PragmaFunction::PragmaStatement("clear_lineage", PragmaClearLineage);
    ExtensionUtil::RegisterFunction(db_instance, clear_lineage_fun);
    
    auto set_persist_fun = PragmaFunction::PragmaCall("set_persist_lineage", PragmaSetPersistLineage, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_persist_fun);
    
    auto set_lineage_fun = PragmaFunction::PragmaCall("set_lineage", PragmaSetLineage, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_lineage_fun);
    

    TableFunction global_func("global_lineage", {}, LineageGFunction::LineageGImplementation,
        LineageGFunction::LineageGBind, LineageGFunction::LineageGInit);
    
    ExtensionUtil::RegisterFunction(db_instance, global_func);
    
    TableFunction lq_func("LQ", {LogicalType::INTEGER}, LQFunction::LQImplementation,
        LQFunction::LQBind, LQFunction::LQInit);
    ExtensionUtil::RegisterFunction(db_instance, lq_func);

    TableFunction pe_func("PolyEval", {}, PolyEvalFunction::PolyEvalImplementation,
        PolyEvalFunction::PolyEvalBind, PolyEvalFunction::PolyEvalInit);
    ExtensionUtil::RegisterFunction(db_instance, pe_func);

    // TODO: group into single function defined in fade.cpp
    // qid, agg id
    TableFunction fade_reader("fade_reader", {LogicalType::INTEGER, LogicalType::INTEGER},
        FadeReaderFunction::FadeReaderImplementation, FadeReaderFunction::FadeReaderBind);
    ExtensionUtil::RegisterFunction(db_instance, fade_reader);
    
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

    // JSON replacement scan
    auto &config = DBConfig::GetConfig(*db.instance);
    config.replacement_scans.emplace_back(LineageScanFunction::ReadLineageReplacement);
}

extern "C" {
DUCKDB_EXTENSION_API void lineage_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::LineageExtension>();
}

DUCKDB_EXTENSION_API const char *lineage_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

} // namespace duckdb
