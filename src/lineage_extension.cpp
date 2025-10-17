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
std::unordered_map<string, vector<vector<idx_t>>> LineageState::lineage_global_store;
std::unordered_map<string, LogicalOperatorType> LineageState::lineage_types;
std::unordered_map<idx_t, unordered_map<idx_t, unique_ptr<LineageInfoNode>>> LineageState::qid_plans;
std::unordered_map<idx_t, idx_t> LineageState::qid_plans_roots;
thread_local ArtifactsLog* LineageState::active_log = nullptr;
thread_local string LineageState::active_log_key = "";
unordered_map<string, shared_ptr<ArtifactsLog>> LineageState::logs;

std::string LineageExtension::Name() {
    return "lineage";
}

void InitThreadLocalLog(string qid_opid_tid) {
  // Initialize thread-local log once
  if (!LineageState::active_log || LineageState::active_log_key != qid_opid_tid) {
    std::lock_guard<std::mutex> guard(LineageState::g_log_lock);
    auto &shared_log = LineageState::logs[qid_opid_tid];  // creates default nullptr if not present
    if (!shared_log) {
        shared_log = make_shared_ptr<ArtifactsLog>();
    }
    LineageState::active_log_key = qid_opid_tid;
    LineageState::active_log = shared_log.get();  // assign thread-local pointer
  }
}

inline void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  // std::cout << "Lineage Clear" << std::endl;
  LineageGlobal::a.clear();
  LineageState::lineage_store.clear();
  LineageState::lineage_types.clear();
  LineageState::qid_plans_roots.clear();
  LineageState::qid_plans.clear();

  LineageState::active_log_key = "";
  LineageState::active_log = nullptr;
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


struct LineageUDAAggState {};

struct LineageUDAInitFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
	}
	static bool IgnoreNull() {
		return false;
	}
};

struct LineageUDABindData : public FunctionData {
	LogicalType return_type;
  idx_t operator_id;

	explicit LineageUDABindData(LogicalType return_type_p, idx_t op_id)
	    : return_type(std::move(return_type_p)), operator_id(op_id) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<LineageUDABindData>(return_type, operator_id);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LineageUDABindData>();
		return return_type == other.return_type && operator_id == other.operator_id;
	}
};

unique_ptr<FunctionData> LineageUDABindFunction(ClientContext &context,
                                           AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
  //D_ASSERT(arguments.size() == 1);

	/*auto &arg_type = arguments[0]->return_type;
	if (arg_type.id() != LogicalTypeId::BIGINT) {
		throw BinderException("lineageUDA_agg requires a ROWID input column");
	}*/

 vector<LogicalType> arg_types;
  for (auto &arg : arguments) {
      arg_types.push_back(arg->return_type);
  }

  // Optionally store argument count or types in bind data
  idx_t arg_count = arg_types.size();

  // Set the function's argument types dynamically
  function.arguments = arg_types;

  // TODO: pass original operator id
  static atomic<idx_t> global_operator_counter{0};
  idx_t operator_id = global_operator_counter++;
  if (LineageState::debug)
    std::cout << "[LineageUDABindFunction] Created bind data with operator_id = "
                << operator_id << std::endl;

	function.return_type = LogicalType::BOOLEAN;

	return make_uniq<LineageUDABindData>(function.return_type, operator_id);
}



void PrintLoggedVector(const IVector &entry, idx_t type_size) {
    std::cout << "IVector count: " << entry.count << "\n";
    // --- Print selection vector ---
    if (entry.sel) {
        std::cout << "Selection vector: ";
        for (idx_t i = 0; i < entry.count; i++) {
            std::cout << entry.sel[i] << " ";
        }
        std::cout << "\n";
    } else {
        std::cout << "Selection vector: nullptr (flat)\n";
    }

    // --- Print data buffer ---
    std::cout << "Data buffer: ";
    for (idx_t i = 0; i < entry.count; i++) {
        idx_t idx = entry.sel ? entry.sel[i] : i;

        // print according to type size
        switch (type_size) {
            case 1:
                std::cout << +reinterpret_cast<uint8_t*>(entry.data)[idx] << " ";
                break;
            case 2:
                std::cout << *reinterpret_cast<uint16_t*>(reinterpret_cast<uint8_t*>(entry.data) + idx*2) << " ";
                break;
            case 4:
                std::cout << *reinterpret_cast<uint32_t*>(reinterpret_cast<uint8_t*>(entry.data) + idx*4) << " ";
                break;
            case 8:
                std::cout << *reinterpret_cast<uint64_t*>(reinterpret_cast<uint8_t*>(entry.data) + idx*8) << " ";
                break;
            default:
                std::cout << "[raw]";
                break;
        }
    }
    std::cout << "\n";
}

void LogVector(Vector &vec, idx_t count, IVector &entry) {
    auto type_size = GetTypeIdSize(vec.GetType().InternalType());
    UnifiedVectorFormat udata;
    vec.ToUnifiedFormat(count, udata);
    entry.count = count;

    // --- Handle Selection Vector ---
    if (!udata.validity.AllValid() && udata.sel) {
			  entry.sel = (sel_t*)malloc(count * sizeof(sel_t));
        memcpy(entry.sel, udata.sel->data(),  count * sizeof(sel_t));
        entry.is_valid = false;
    } else {
        entry.sel = nullptr;
        entry.is_valid = true;
    }

    // --- Handle Data ---
    entry.data = (data_ptr_t)malloc(type_size * count);  // owns memory
    memcpy(entry.data, udata.data, type_size * count);
    
    if (LineageState::debug)
      PrintLoggedVector(entry, type_size);
}


static void LineageUDAUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                Vector &state_vector, idx_t count) {
  D_ASSERT(input_count >= 1);
  D_ASSERT(state_vector.GetVectorType() == VectorType::FLAT_VECTOR);
  
  idx_t thread_id = GetThreadId();
  auto &bind = aggr_input_data.bind_data->Cast<LineageUDABindData>();
  
  if (LineageState::debug)
    std::cout << "[Update] Operator " << bind.operator_id
              << " processing " << count << " rows " << thread_id << std::endl;
  string qid_opid_tid = to_string(bind.operator_id) + "_" + to_string(thread_id);
  InitThreadLocalLog(qid_opid_tid);

  if (!LineageState::persist) return;

  if (LineageState::use_vector) {
    Vector addresses(state_vector.GetType());
    VectorOperations::Copy(state_vector, addresses, count, 0, 0);
    if (LineageState::debug)
      std::cout << "addresses vector: " << addresses.ToString(count) << std::endl;

    vector<Vector> annotations_list;
    annotations_list.reserve(input_count);
    for (idx_t i = 0; i < input_count; i++) {
      Vector annotations(inputs[i].GetType());
      VectorOperations::Copy(inputs[i], annotations, count, 0, 0);
      if (LineageState::debug)
        std::cout << "annotations vector: " << annotations.ToString(count) << std::endl;
      annotations_list.push_back(std::move(annotations));
    }
      
    LineageState::active_log->agg_update_log.push_back({std::move(addresses), std::move(annotations_list)});
  } else {
    LineageState::active_log->buffer_agg_update_log.emplace_back();
    {
      auto& entry = LineageState::active_log->buffer_agg_update_log.back().first;
      LogVector(state_vector, count, entry);
    }
    {
      vector<IVector>& annotations_list = LineageState::active_log->buffer_agg_update_log.back().second;
      annotations_list.resize(input_count);

      for (idx_t i = 0; i < input_count; i++) {
        auto& entry = annotations_list[i];
        LogVector(inputs[i], count, entry);
      }
    }
  }

}


static void LineageUDACombineFunction(Vector &states_vector, Vector &combined, AggregateInputData &aggr_input_data,
                                 idx_t count) {
  idx_t thread_id = GetThreadId();
  auto &bind = aggr_input_data.bind_data->Cast<LineageUDABindData>();
  if (LineageState::debug)
    std::cout << "[Combine] Operator " << bind.operator_id
                << " processing " << count << " rows " << thread_id << std::endl;

  string qid_opid_tid = to_string(bind.operator_id) + "_" + to_string(thread_id);
  InitThreadLocalLog(qid_opid_tid);
  
  if (!LineageState::persist) return;

  if (LineageState::use_vector) {
    Vector source(states_vector.GetType());
    VectorOperations::Copy(states_vector, source, count, 0, 0);
    Vector target(combined.GetType());
    VectorOperations::Copy(combined, target, count, 0, 0);
  
    if (LineageState::debug) {
      std::cout << "source addresses vector: " << source.ToString(count) << std::endl;
      std::cout << "target addresses vector: " << target.ToString(count) << std::endl;
    }
    LineageState::active_log->agg_combine_log.push_back({std::move(source), std::move(target)});
  } else {
    LineageState::active_log->buffer_agg_combine_log.emplace_back();
    {
      auto& entry = LineageState::active_log->buffer_agg_combine_log.back().first;
      LogVector(states_vector, count, entry);
    }
    {
      auto& entry = LineageState::active_log->buffer_agg_combine_log.back().second;
      LogVector(combined, count, entry);
    }
  }
}

static void LineageUDAFinalize(Vector &states_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                          idx_t offset) {
  // result should be BOOLEAN
	D_ASSERT(result.GetType().id() == LogicalTypeId::BOOLEAN);

	auto result_data = FlatVector::GetData<bool>(result);
	auto &mask = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		result_data[rid] = true;
	}
  
  idx_t thread_id = GetThreadId();
  auto &bind = aggr_input_data.bind_data->Cast<LineageUDABindData>();
  if (LineageState::debug)
    std::cout << "[Finalize] Operator " << bind.operator_id
                << " processing " << count << " rows " << thread_id << std::endl;

  string qid_opid_tid = to_string(bind.operator_id) + "_" + to_string(thread_id);
  InitThreadLocalLog(qid_opid_tid);
  
  if (!LineageState::persist) return;

  if (LineageState::use_vector) {
    Vector addresses(states_vector.GetType());
    VectorOperations::Copy(states_vector, addresses, count, 0, 0);
    if (LineageState::debug)
      std::cout << "finalize addresses vector: " << addresses.ToString(count) << std::endl;

    LineageState::active_log->agg_finalize_log.push_back(std::move(addresses));
  } else {
    LineageState::active_log->buffer_agg_finalize_log.emplace_back();
    {
      auto& entry = LineageState::active_log->buffer_agg_finalize_log.back();
      LogVector(states_vector, count, entry);
    }
  }
}

AggregateFunction GetLineageUDAAllEvenFunction() {
	AggregateFunction func(
	    {},
	    LogicalTypeId::BOOLEAN,               // return type
	    AggregateFunction::StateSize<LineageUDAAggState>,                    // state size helper
	    AggregateFunction::StateInitialize<LineageUDAAggState, LineageUDAInitFunction>, // initializer
	    LineageUDAUpdateFunction,                  // update
	    LineageUDACombineFunction,                 // combine
	    LineageUDAFinalize,                        // finalize
	    nullptr,
      LineageUDABindFunction,                     // bind function
      nullptr, nullptr, nullptr   
	);

  // mark as variadic
  func.varargs = LogicalType::ANY;

	return func;
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
