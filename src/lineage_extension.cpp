#define DUCKDB_EXTENSION_MAIN
#include "lineage_extension.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"
#include "lineage/lineage_meta.hpp"
#include "lineage/lineage_reader.hpp"
#include "lineage/lineage_global.hpp"
#include "lineage/lineage_query.hpp"

#include "fade/fade.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/function/aggregate_function.hpp"

// TODO: replace with "duckdb/execution/lineage_logger.hpp"
#include "duckdb/execution/lineage_logger.hpp"
// #include "lineage_logger.hpp"

namespace duckdb {

bool LineageState::use_vector = false;
bool LineageState::cache = false;
bool LineageState::capture = false;
bool LineageState::hybrid= false;
bool LineageState::debug = false;
bool LineageState::use_internal_lineage = false;
bool LineageState::persist = true;
std::mutex LineageState::g_log_lock;
std::unordered_map<string, vector<std::pair<Vector, int>>> LineageState::lineage_store;
std::unordered_map<string, vector<IVector>> LineageState::lineage_store_buf;
std::unordered_map<string, vector<vector<idx_t>>> LineageState::lineage_global_store;
std::unordered_map<string, LogicalOperatorType> LineageState::lineage_types;
std::unordered_map<idx_t, unordered_map<idx_t, unique_ptr<LineageInfoNode>>> LineageState::qid_plans;
std::unordered_map<idx_t, idx_t> LineageState::qid_plans_roots;
unordered_map<string, unique_ptr<PartitionedLineage>> LineageState::partitioned_store_buf;

std::string LineageExtension::Name() {
    return "lineage";
}

inline void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  std::cout << "Lineage Clear" << std::endl;
  LineageGlobal::a.clear();
  LineageState::lineage_store.clear();
  LineageState::lineage_types.clear();
  LineageState::qid_plans_roots.clear();
  LineageState::qid_plans.clear();

  LineageState::lineage_global_store.clear();
  for (auto& e : LineageState::lineage_store_buf) {
    for (auto& e2 : e.second) {
      e2.clear();
    }
  }
  LineageState::lineage_store_buf.clear();
  for (auto& e : LineageState::partitioned_store_buf) {
    e.second->clear();
  }
  LineageState::partitioned_store_buf.clear();

  LineageState::active_log_key = "";
  LineageState::active_log = nullptr;
  for (auto& e : LineageState::logs) {
    e.second->clear();
  }
  LineageState::logs.clear();

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

  for (auto& fade_res : FadeState::fade_results) {
    for (auto& vars : fade_res.second.alloc_typ_vars) {
      for (auto & vars_t : vars.second.second) {
        if (vars.second.first == LogicalType::INTEGER) {
          delete static_cast<int*>(vars_t);
        } else {
          delete static_cast<float*>(vars_t);
        }
      }
    }
  }

  FadeState::fade_results.clear();

  FadeState::codes.clear();
  FadeState::cached_spec_map.clear();
  FadeState::cached_spec_stack.clear();
}

inline void PragmaLineageDebug(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::debug  = parameters.values[0].GetValue<bool>();
}

inline void PragmaSetPersistLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::persist  = parameters.values[0].GetValue<bool>();
}

inline void PragmaSetUseInternalLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::use_internal_lineage  = parameters.values[0].GetValue<bool>();
}

inline void PragmaSetLineage(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::capture  = parameters.values[0].GetValue<bool>();
  if (LineageState::capture) {
    LineageGlobal::LS.capture = LineageState::hybrid;
  } else {
    LineageGlobal::LS.capture = false;
  }
}

inline void PragmaSetUseVector(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::use_vector  = parameters.values[0].GetValue<bool>();
}


inline void PragmaSetHybrid(ClientContext &context, const FunctionParameters &parameters) {
  LineageState::hybrid  = parameters.values[0].GetValue<bool>();
  if (LineageState::capture) {
    LineageGlobal::LS.capture = LineageState::hybrid;
  } else {
    LineageGlobal::LS.capture = false;
  }
}

static void PragmaDisableFilterPushDown(ClientContext &context, const FunctionParameters &parameters) {
  LineageGlobal::enable_filter_pushdown = false;
  std::cout << "Disable Filter Pushdown" << std::endl;
}

static void PragmaEnableFilterPushDown(ClientContext &context, const FunctionParameters &parameters) {
  LineageGlobal::enable_filter_pushdown = true;
	std::cout << "Enable Filter Pushdown" << std::endl;
}

static void PragmaSetAgg(ClientContext &context, const FunctionParameters &parameters) {
	string agg_type = parameters.values[0].ToString();
	D_ASSERT(agg_type == "perfect" || agg_type == "reg" || agg_type == "clear");
	std::cout << "Setting agg type to " << agg_type << " - be careful! Failures possible if too many buckets (I think)." << std::endl;
	if (agg_type == "clear") {
    LineageGlobal::explicit_agg_type = "";
	} else {
    LineageGlobal::explicit_agg_type = agg_type;
	}
}

static string PragmaSetJoin(ClientContext &context, const FunctionParameters &parameters) {
	string join_type = parameters.values[0].ToString();
	D_ASSERT(join_type == "hash" || join_type == "merge" || join_type == "nl" || join_type == "index" || join_type == "block" || join_type == "clear");
	std::cout << "Setting join type to " << join_type << " - be careful! Failures possible for hash/index join if non equijoin." << std::endl;
	if (join_type == "clear") {
    LineageGlobal::explicit_join_type = "";
	} else {
    LineageGlobal::explicit_join_type = join_type;
	}
  return "select 1";
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

    auto debug_fun = PragmaFunction::PragmaCall("set_debug_lineage", PragmaLineageDebug, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, debug_fun);

    auto clear_lineage_fun = PragmaFunction::PragmaStatement("clear_lineage", PragmaClearLineage);
    ExtensionUtil::RegisterFunction(db_instance, clear_lineage_fun);
    
    auto set_persist_fun = PragmaFunction::PragmaCall("set_persist_lineage", PragmaSetPersistLineage, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_persist_fun);

    auto set_use_internal_lineage_fun = PragmaFunction::PragmaCall("set_use_internal_lineage", PragmaSetUseInternalLineage, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_use_internal_lineage_fun);
    
    auto set_lineage_fun = PragmaFunction::PragmaCall("set_lineage", PragmaSetLineage, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_lineage_fun);
    
    auto set_hybrid_fun = PragmaFunction::PragmaCall("set_hybrid", PragmaSetHybrid, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_hybrid_fun);
    
    auto set_use_vector_fun = PragmaFunction::PragmaCall("set_use_vector", PragmaSetUseVector, {LogicalType::BOOLEAN});
    ExtensionUtil::RegisterFunction(db_instance, set_use_vector_fun);

    auto enable_filter_scan = PragmaFunction::PragmaStatement("enable_filter_pushdown", PragmaEnableFilterPushDown);
    ExtensionUtil::RegisterFunction(db_instance, enable_filter_scan);
    auto disable_filter_scan = PragmaFunction::PragmaStatement("disable_filter_pushdown", PragmaDisableFilterPushDown);
    ExtensionUtil::RegisterFunction(db_instance, disable_filter_scan);
	  auto set_agg_fun = PragmaFunction::PragmaCall("set_agg", PragmaSetAgg, {LogicalType::VARCHAR});
    ExtensionUtil::RegisterFunction(db_instance, set_agg_fun);
	  auto set_join_fun = PragmaFunction::PragmaCall("set_join", PragmaSetJoin, {LogicalType::VARCHAR});
    ExtensionUtil::RegisterFunction(db_instance, set_join_fun);

    TableFunction global_func("global_lineage", {}, LineageGFunction::LineageGImplementation,
        LineageGFunction::LineageGBind, LineageGFunction::LineageGInit);
    ExtensionUtil::RegisterFunction(db_instance, global_func);
    
    TableFunction lq_func("LQ", {LogicalType::INTEGER}, LQFunction::LQImplementation,
        LQFunction::LQBind, LQFunction::LQInit);
    ExtensionUtil::RegisterFunction(db_instance, lq_func);

    InitFuncs(db_instance);

    // JSON replacement scan
    auto &config = DBConfig::GetConfig(*db.instance);
    config.replacement_scans.emplace_back(LineageScanFunction::ReadLineageReplacement);


    AggregateFunctionSet set("internal_lineage");
    set.AddFunction(GetLineageUDAAllEvenFunction());
    ExtensionUtil::RegisterFunction(db_instance, set);
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
