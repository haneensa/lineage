#define DUCKDB_EXTENSION_MAIN
#include "lineage_extension.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"
#include "lineage/lineage_meta.hpp"
#include "lineage/lineage_blocks_reader.hpp"
#include "lineage/lineage_reader.hpp"
#include "lineage/lineage_global.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

bool LineageState::cache = false;
bool LineageState::capture = false;
bool LineageState::debug = false;
bool LineageState::persist = true;
std::mutex LineageState::g_log_lock;
std::unordered_map<string, vector<vector<idx_t>>> LineageState::lineage_global_store;
std::unordered_map<string, LogicalOperatorType> LineageState::lineage_types;
std::unordered_map<idx_t, unordered_map<idx_t, unique_ptr<LineageInfoNode>>> LineageState::qid_plans;
std::unordered_map<idx_t, idx_t> LineageState::qid_plans_roots;
unordered_map<string, unique_ptr<PartitionedLineage>> LineageState::partitioned_store_buf;
unordered_map<idx_t, vector<JoinAggBlocks>> LineageState::lineage_blocks;

std::string LineageExtension::Name() {
    return "lineage";
}

inline void PragmaPrepareLineage(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  idx_t root_id = LineageState::qid_plans_roots[qid];
  if (LineageState::debug) std::cout << "PRAGMA PrepapreLineage: " << qid << " " << root_id << 
  " " << EnumUtil::ToChars<LogicalOperatorType>(LineageState::qid_plans[qid][root_id]->type)
  << std::endl;
  InitGlobalLineageBuff(context, qid, root_id);
  // persist these. use blocks reader to return the relational representation
  vector<JoinAggBlocks>& lineage_blocks = LineageState::lineage_blocks[qid];
  lineage_blocks.emplace_back();
  CreateJoinAggBlocks(qid, root_id, lineage_blocks, {}, 0);
}


inline void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::lineage_types.clear();
  LineageState::qid_plans_roots.clear();
  LineageState::qid_plans.clear();

  LineageState::lineage_global_store.clear();
  for (auto& e : LineageState::partitioned_store_buf) {
    e.second->clear();
  }
  LineageState::partitioned_store_buf.clear();
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
      if (LineageState::debug)
        std::cout << "Plan prior to modifications: \n" << plan->ToString() << std::endl;
      plan = AddLineage(input, plan);
      if (LineageState::debug)
        std::cout << "Plan after to modifications: \n" << plan->ToString() << std::endl;
    };

    auto &db_instance = *db.instance;
    db_instance.config.optimizer_extensions.emplace_back(*optimizer_extension);
    
  	ExtensionUtil::RegisterFunction(db_instance, LineageScanFunction::GetFunctionSet());

    TableFunction pragma_func("pragma_latest_qid", {}, LineageMetaFunction::LineageMetaImplementation, 
        LineageMetaFunction::LineageMetaBind);
    ExtensionUtil::RegisterFunction(db_instance, pragma_func);
    
    TableFunction pragma_func_block("read_block", {LogicalType::INTEGER},
        BlockReaderFunction::Implementation, BlockReaderFunction::Bind);
    ExtensionUtil::RegisterFunction(db_instance, pragma_func_block);

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
    
    auto prepare_lineage_fun = PragmaFunction::PragmaCall("PrepareLineage",
        PragmaPrepareLineage, {LogicalType::INTEGER});
    ExtensionUtil::RegisterFunction(db_instance, prepare_lineage_fun);

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
