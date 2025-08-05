#define DUCKDB_EXTENSION_MAIN
#include "lineage_extension.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"
#include "lineage/lineage_meta.hpp"
#include "lineage/lineage_reader.hpp"
#include "lineage/lineage_global.hpp"
#include "lineage/lineage_query.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

bool LineageState::capture = false;
bool LineageState::debug = false;
bool LineageState::persist = true;
std::unordered_map<string, vector<std::pair<Vector, int>>> LineageState::lineage_store;
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

void LineageExtension::Load(DuckDB &db) {
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
    
    TableFunction lq_func("LQ", {}, LQFunction::LQImplementation,
        LQFunction::LQBind, LQFunction::LQInit);
    
    ExtensionUtil::RegisterFunction(db_instance, lq_func);

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
