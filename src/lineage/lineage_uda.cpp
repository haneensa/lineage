#include "lineage_extension.hpp"


#include "lineage/lineage_init.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/function/aggregate_function.hpp"

#include <iostream>

namespace duckdb {

thread_local ArtifactsLog* LineageState::active_log = nullptr;
thread_local string LineageState::active_log_key = "";
unordered_map<string, unique_ptr<ArtifactsLog>> LineageState::logs;

// TODO: avoid using global var, figure out how to use op state
void InitThreadLocalLog(string qid_opid_tid) {
  // Initialize thread-local log once
  if (!LineageState::active_log || LineageState::active_log_key != qid_opid_tid
      || LineageState::logs.find(qid_opid_tid) == LineageState::logs.end()) {
    std::lock_guard<std::mutex> guard(LineageState::g_log_lock);
    auto &shared_log = LineageState::logs[qid_opid_tid];  // creates default nullptr if not present
    if (!shared_log) {
        shared_log = make_uniq<ArtifactsLog>();
    }
    LineageState::active_log_key = qid_opid_tid;
    LineageState::active_log = shared_log.get();  // assign thread-local pointer
  }  
}

struct LineageUDAAggState {};

struct LineageUDAInitFunction {
	template <class STATE>
	static void Initialize(STATE &state) {}
	static bool IgnoreNull() {
		return false;
	}
};

unique_ptr<FunctionData> LineageUDABindFunction(ClientContext &context,
                                           AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {

  Value v = ExpressionExecutor::EvaluateScalar(context, *arguments.back());
  int64_t operator_id = v.GetValue<int64_t>();
  arguments.pop_back();

  vector<LogicalType> arg_types;
  for (auto &arg : arguments) {
      arg_types.push_back(arg->return_type);
  }

  // Optionally store argument count or types in bind data
  idx_t arg_count = arg_types.size();

  // Set the function's argument types dynamically
  function.arguments = arg_types;
	function.return_type = LogicalType::BIGINT;
	return make_uniq<LineageUDABindData>(function.return_type, operator_id); 
}

static void LineageUDAUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                Vector &state_vector, idx_t count) {
  D_ASSERT(input_count >= 1);
  D_ASSERT(state_vector.GetVectorType() == VectorType::FLAT_VECTOR);
  
  idx_t thread_id = GetThreadId();
  auto &bind = aggr_input_data.bind_data->Cast<LineageUDABindData>();
  
  if (LineageState::debug) {
    std::cout << "[Update] Operator " << bind.operator_id
              << " processing " << count << " rows " << thread_id << std::endl;
  }
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
    idx_t lsn = LineageState::active_log->buffer_agg_update_log.size() - 1;
    auto& u_entry = LineageState::active_log->buffer_agg_update_log[lsn];
    {
      auto& entry = u_entry.first;
      LogVector(state_vector, count, entry);
    }
    {
      vector<IVector>& annotations_list = u_entry.second;
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
    if (LineageState::debug) {
      std::cout << "source addresses vector: " << states_vector.ToString(count) << std::endl;
      std::cout << "target addresses vector: " << combined.ToString(count) << std::endl;
    }

  if (LineageState::use_vector) {
    Vector source(states_vector.GetType());
    VectorOperations::Copy(states_vector, source, count, 0, 0);
    Vector target(combined.GetType());
    VectorOperations::Copy(combined, target, count, 0, 0);
    LineageState::active_log->agg_combine_log.push_back({std::move(source), std::move(target)});
  } else {
    idx_t lsn = 0;
    LineageState::active_log->buffer_agg_combine_log.emplace_back();
    lsn = LineageState::active_log->buffer_agg_combine_log.size() - 1;
    {
      auto& entry = LineageState::active_log->buffer_agg_combine_log[lsn].first;
      LogVector(states_vector, count, entry);
    }
    {
      auto& entry = LineageState::active_log->buffer_agg_combine_log[lsn].second;
      LogVector(combined, count, entry);
    }
  }
}

static void LineageUDAFinalize(Vector &states_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                          idx_t offset) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::BIGINT);
  auto result_data = FlatVector::GetData<int64_t>(result);
  auto &mask = FlatVector::Validity(result);
  auto &bind = aggr_input_data.bind_data->Cast<LineageUDABindData>();
  idx_t g_offset = bind.g_offset.fetch_add(count);

  for (idx_t i = 0; i < count; i++) {
     const auto rid = i + offset;
     result_data[rid] = i+g_offset; //true;
  }

  idx_t thread_id = GetThreadId();
  if (LineageState::debug) {
    std::cout << "[Finalize] Operator " << bind.operator_id
                << " processing " << count << " rows " << thread_id <<
                " " << offset << " " << g_offset << std::endl;
  }
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
    idx_t lsn = 0;
    LineageState::active_log->buffer_agg_finalize_log.emplace_back();
    lsn = LineageState::active_log->buffer_agg_finalize_log.size() - 1;
    auto& f_entry = LineageState::active_log->buffer_agg_finalize_log[lsn];
    f_entry.second = g_offset;
    LogVector(states_vector, count, f_entry.first);
  }
}

AggregateFunction GetLineageUDAAllEvenFunction() {
	AggregateFunction func(
	    {},
	    LogicalTypeId::BIGINT,               // return type
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


} // namespace duckdb
