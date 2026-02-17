#define DUCKDB_EXTENSION_MAIN
// lineage_extension.cpp
//
// Entry point for the Lineage DuckDB extension.
//
// Responsibilities:
//  - Define and initialize global lineage state
//  - Hook into the DuckDB optimizer to inject lineage capture operators
//  - Register pragma commands for controlling lineage behavior
//  - Register table functions for reading lineage results
//
// This file does NOT implement lineage capture itself;
// it wires together components implemented under lineage/*.

#include "lineage_extension.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"
#include "lineage/lineage_meta.hpp"
#include "lineage/lineage_blocks_reader.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

// ---- Global lineage configuration flags ----
bool LineageState::capture = false;
bool LineageState::debug = false;
bool LineageState::persist = true;

// ---- Logical plan metadata (keyed by query id (QID), operator id (OPID)) ----
std::unordered_map<QID_OPID, LogicalOperatorType> LineageState::lineage_types;
std::unordered_map<QID, unordered_map<OPID, unique_ptr<LineageInfoNode>>> LineageState::qid_plans;
std::unordered_map<QID, OPID> LineageState::qid_plans_roots;

// ---- Lineage storage ----
std::unordered_map<QID_OPID, vector<vector<idx_t>>> LineageState::lineage_global_store;
unordered_map<QID_OPID, unique_ptr<PartitionedLineage>> LineageState::partitioned_store_buf;
unordered_map<QID, vector<JoinAggBlocks>> LineageState::lineage_blocks;

// ---- Synchronization ----
std::mutex LineageState::g_log_lock;


std::string LineageExtension::Name() {
    return "lineage";
}

// Prepare lineage structures for a completed query.
//
// Given a query id (qid), this pragma:
//  - Finds the root logical operator of the query plan
//  - Initializes global lineage buffers - unified physical representation
//  - Materializes Join/Aggregation lineage blocks
//
// This must be called *after* the query has executed.
inline void PragmaPrepareLineage(ClientContext &context, const FunctionParameters &parameters) {
	int qid = parameters.values[0].GetValue<int>();
  // root logical operator id for this query
  idx_t root_id = LineageState::qid_plans_roots[qid]; // TODO: handle cases where qid not valid
  LDEBUG("PRAGMA PrepapreLineage: ", qid, " ", root_id,
    " ", EnumUtil::ToChars<LogicalOperatorType>(LineageState::qid_plans[qid][root_id]->type));
  InitGlobalLineageBuff(context, qid, root_id);
  vector<JoinAggBlocks>& lineage_blocks = LineageState::lineage_blocks[qid];
  lineage_blocks.emplace_back();
  CreateJoinAggBlocks(qid, root_id, lineage_blocks, {}, 0);
}


inline void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::Clear();
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
      // Only inject lineage for SPJUA queries when capture is enabled
      if (!LineageState::capture || !IsSPJUA(plan)) return;
      if (LineageState::debug)
        std::cout << "Plan prior to modifications: \n" << plan->ToString() << std::endl;

      // Rewrite logical plan to include lineage capture operators
      plan = AddLineage(input, plan);
      if (LineageState::debug)
        std::cout << "Plan after to modifications: \n" << plan->ToString() << std::endl;
    };

    auto &db_instance = *db.instance;
    db_instance.config.optimizer_extensions.emplace_back(*optimizer_extension);
    
    TableFunction pragma_func("lineage_meta", {}, LineageMetaFunction::Implementation, 
        LineageMetaFunction::Bind);
    ExtensionUtil::RegisterFunction(db_instance, pragma_func);
    
    TableFunction tfunc_block("read_block", {LogicalType::INTEGER},
        BlockReaderFunction::Implementation, BlockReaderFunction::Bind);
    ExtensionUtil::RegisterFunction(db_instance, tfunc_block);

    auto debug_fun = PragmaFunction::PragmaCall("set_debug_lineage", PragmaLineageDebug, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, debug_fun);

    auto clear_lineage_fun = PragmaFunction::PragmaStatement("clear_lineage", PragmaClearLineage);
    ExtensionUtil::RegisterFunction(db_instance, clear_lineage_fun);
    
    auto set_persist_fun = PragmaFunction::PragmaCall("set_persist_lineage", PragmaSetPersistLineage, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_persist_fun);

    auto set_lineage_fun = PragmaFunction::PragmaCall("set_lineage", PragmaSetLineage, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_lineage_fun);
    
    auto prepare_lineage_fun = PragmaFunction::PragmaCall("PrepareLineage",
        PragmaPrepareLineage, {LogicalType::INTEGER});
    ExtensionUtil::RegisterFunction(db_instance, prepare_lineage_fun);
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
