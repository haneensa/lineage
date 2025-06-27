// TODO: 1) manage how to get the right query_id [DONE]
//       3) refactor aggs
//       4) return post and pre results
//       5) multi-threading support
//       2) pass not to return only the removed results [DONE]
//       5) enable to pass projection over specs
//       6) single intervention support
//       7) dense intervention support
//       8) compile and link statically templates for dense interventions
#define DUCKDB_EXTENSION_MAIN
#include "fade_extension.hpp"

#include <iostream>

#include "fade/fade_reader.hpp"
#include "fade/fade_get_predicate.hpp"
#include "lineage/lineage_init.hpp"
#include "lineage/lineage_meta.hpp"
#include "lineage/lineage_reader.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

idx_t FadeState::num_worker = 1;
unique_ptr<FadeNode> FadeState::cached_fade_result;
unordered_map<string, idx_t> FadeState::table_count;
unordered_map<string, vector<string>> FadeState::cached_spec_map;
// ### for sparse
unordered_map<string, idx_t> FadeState::col_n_unique;
unordered_map<string, unordered_map<string, vector<int32_t>>> FadeState::table_col_annotations;
bool FadeState::is_equal = true;
unordered_map<string, unique_ptr<MaterializedQueryResult>> FadeState::codes;
vector<string> FadeState::cached_spec_stack;
// ##############

idx_t LineageState::query_id = 0;
bool LineageState::capture = false;
bool LineageState::debug = false;
bool LineageState::persist = true;
std::unordered_map<string, vector<std::pair<Vector, int>>> LineageState::lineage_store;
std::unordered_map<string, LogicalOperatorType> LineageState::lineage_types;
std::unordered_map<idx_t, unordered_map<idx_t, unique_ptr<LineageInfoNode>>> LineageState::qid_plans;
std::unordered_map<idx_t, idx_t> LineageState::qid_plans_roots;


std::string FadeExtension::Name() {
    return "fade";
}

inline void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::lineage_store.clear();
  LineageState::lineage_types.clear();
  LineageState::qid_plans_roots.clear();
  LineageState::qid_plans.clear();
  FadeState::cached_spec_stack.clear();
  LineageState::query_id = 0;
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

// 1) prepapre_lineage: query id
inline void PragmaPrepareLineage(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  idx_t root_id = LineageState::qid_plans_roots[qid];
  get_cached_lineage(qid, root_id);
  get_cached_vals(qid, root_id);
  compute_count_sum_sum2(qid, root_id);
}


// 2) whatif
inline void PragmaFade(ClientContext &context, const FunctionParameters &parameters) {
  FadeState::cached_spec_stack.clear();
  int qid = parameters.values[0].GetValue<int>();
  idx_t agg_idx = parameters.values[1].GetValue<int>();
  auto list_values = ListValue::GetChildren(parameters.values[2]);
  auto spec_values = ListValue::GetChildren(parameters.values[3]);
  FadeState::is_equal = parameters.values[4].GetValue<bool>();

  vector<int> groups;
  for (idx_t i = 0; i < list_values.size(); ++i) {
      auto &child = list_values[i];
      groups.push_back(child.GetValue<int>());
  }
  vector<string> specs;
  for (idx_t i = 0; i < spec_values.size(); ++i) {
    specs.push_back(spec_values[i].ToString());
  }
  if (LineageState::debug)
    std::cout << "Whatif(qid:" << qid << ",n_specs:" << specs.size() << ",agg_idx:" << agg_idx
      << ",ngroups:" << groups.size() << ")" << std::endl;;
  
  // 1. Parse: spec. Input (t.col1|t.col2|..)
  unordered_map<string, vector<string>> spec_map = parse_spec(specs);
  
  // TODO: separate this and make sure it is done once
  // 2. Read: annotations
  read_annotations(context, spec_map);
  
  idx_t root_id = LineageState::qid_plans_roots[qid];
  populate_and_verify_n_input_output(qid, root_id);
  
  AdjustOutputIds(qid, root_id, groups);
  
  WhatIfSparse(context, qid, agg_idx, spec_map, groups);

  
  if (FadeState::cached_fade_result) {
    FadeState::cached_fade_result->groups = std::move(groups);
    FadeState::cached_spec_map = std::move(spec_map);
  }
}


void FadeExtension::Load(DuckDB &db) {
    auto optimizer_extension = make_uniq<OptimizerExtension>();
    optimizer_extension->optimize_function = [](OptimizerExtensionInput &input, 
                                            unique_ptr<LogicalOperator> &plan) {
      if (IsSPJUA(plan) == false || LineageState::capture == false) return;
      if (LineageState::debug) {
        std::cout << "Plan prior to modifications: \n" << plan->ToString() << std::endl;
      }
      plan = AddLineage(input, plan);
      if (LineageState::debug) {
        std::cout << "Plan after to modifications: \n" << plan->ToString() << std::endl;
      } 
    };

    auto &db_instance = *db.instance;
    db_instance.config.optimizer_extensions.emplace_back(*optimizer_extension);
    std::cout << "Fade extension loaded successfully.\n";
    
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
    
    auto prepare_lineage_fun = PragmaFunction::PragmaCall("prepare_lineage", PragmaPrepareLineage, {LogicalType::INTEGER});
    ExtensionUtil::RegisterFunction(db_instance, prepare_lineage_fun);
    
    auto whatif_fun = PragmaFunction::PragmaCall("whatif", PragmaFade, {LogicalType::INTEGER,
        LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER),
        LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, whatif_fun);
  	ExtensionUtil::RegisterFunction(db_instance, FadeReaderFunction::GetFunctionSet());
  	ExtensionUtil::RegisterFunction(db_instance, GetPredicatesFunction::GetFunctionSet());
    
    // JSON replacement scan
    auto &config = DBConfig::GetConfig(*db.instance);
    config.replacement_scans.emplace_back(LineageScanFunction::ReadLineageReplacement);
}

extern "C" {
DUCKDB_EXTENSION_API void fade_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::FadeExtension>();
}

DUCKDB_EXTENSION_API const char *fade_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

} // namespace duckdb
