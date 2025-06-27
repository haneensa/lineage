#include "fade/fade_reader.hpp"

#include "fade_extension.hpp"
#include "lineage/lineage_init.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

#include <iostream>
namespace duckdb {

template<class T>
void read_sum(idx_t index, void* in_data, void* out_data, idx_t col, idx_t limit, idx_t sum_val) {
  T* data_ptr = (T*)in_data;
  T* col_data = (T*)out_data;
  if (FadeState::is_equal) {
    for (int i = 0; i < limit; ++i) {
      col_data[i] = sum_val-data_ptr[index + i];
    }
  } else {
    for (int i = 0; i < limit; ++i) {
      col_data[i] = data_ptr[index + i];
    }
  }
}

template<class T>
void read_avg(idx_t index, void* in_data, int* count_ptr, float* col_data, idx_t col, idx_t limit,
              idx_t sum_val, idx_t count_val) {
  T* data_ptr = (T*)in_data;
  if (FadeState::is_equal) {
    for (int i = 0; i < limit; ++i) {
      // TODO: make sure we don't divide by 0
      col_data[i] = (sum_val - data_ptr[index + i]) / (count_val - count_ptr[index + i]);
    }
  } else {
    for (int i = 0; i < limit; ++i) {
      // TODO: make sure we don't divide by 0
      col_data[i] = (data_ptr[index + i]) / (count_ptr[index + i]);
    }
  }
}

template<class T>
void read_stddev(idx_t index, void* in_data, float* sum_2_ptr, int* count_ptr, float* col_data, idx_t col, idx_t limit,
              idx_t sum_val, idx_t sum_2_val, idx_t count_val) {
  T* sum_ptr = (T*)in_data;
  if (FadeState::is_equal) {
    for (int i = 0; i < limit; ++i) {
      // TODO: make sure we don't divide by 0
      float sum_base = sum_val - sum_ptr[index + i];
      int count_base = count_val - count_ptr[index + i];
      col_data[i] = std::sqrt( (sum_2_val - sum_2_ptr[index + i]) / count_base - (sum_base*sum_base) / (count_base*count_base) );
    }
  } else {
    for (int i = 0; i < limit; ++i) {
      // TODO: make sure we don't divide by 0
      float sum_base = sum_ptr[index + i];
      int count_base = count_ptr[index + i];
      col_data[i] = std::sqrt( (sum_2_ptr[index + i]) / count_base - (sum_base*sum_base) / (count_base*count_base) );
    }
  }
}

void FadeReaderFunction::FadeReaderImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  if (!data_p.local_state) return;
  auto &data = data_p.local_state->Cast<FadeReaderLocalState>();
  auto &gstate = data_p.global_state->Cast<FadeReaderGlobalState>();
  auto &bind_data = data_p.bind_data->CastNoConst<FadeReaderBindData>();

  if (FadeState::cached_fade_result == nullptr) return;
  auto &fnode = FadeState::cached_fade_result;
  idx_t out_var = bind_data.out_var;  // 1) get out_var idx
  auto &lop_info = LineageState::qid_plans[fnode->qid][fnode->opid];
  string fname = bind_data.fname;
  string out_key = fname + "_" + to_string(out_var);

  // std::cout << "fade reader implementation " << fname << " " << out_var << " " << out_key << std::endl;
	if (data.offset >= bind_data.n_interventions)	return; // finished returning values

  // start returning values
  // either fill up the chunk or return all the remaining columns
  idx_t col = 1;
	idx_t count = 0;
  int limit = bind_data.n_interventions - data.offset;
  if (limit >= STANDARD_VECTOR_SIZE) {
    limit = STANDARD_VECTOR_SIZE;
  }
  
  // if is_equal == false then don't subtract
  auto &sub_aggs = lop_info->agg_info->sub_aggs;
  auto &agg_context = lop_info->agg_info->aggs[out_var];
  for (int i=0; i < bind_data.n_groups; ++i) {
    int index = i * bind_data.n_interventions + data.offset;
    idx_t g = i;
    if (!fnode->groups.empty()) g = fnode->groups[i];
    // agg->ReadOutput()
    if (fname == "count") {
      int count_val = agg_context->groups_count[g];
      
      output.data[col].Initialize(true, limit);
      int* col_data = (int*)output.data[col].GetData();
      
      string count_agg_key = agg_context->sub_aggs[0];

      int* count_ptr = (int*)fnode->alloc_vars[count_agg_key][0];
      for (int v = 0; v < limit; ++v) {
        col_data[v] = count_val-count_ptr[index + v];
      }
      col++;
      count = limit;
    } else if (fname  == "sum") {
      float sum_val = agg_context->groups_sum[g];

      output.data[col].Initialize(true, limit);
      void* out_data = output.data[col].GetData();
      
      string sum_agg_key = agg_context->sub_aggs[0];

      void* data_ptr = fnode->alloc_vars[sum_agg_key][0];
      if (sub_aggs[ sum_agg_key ]->return_type == LogicalType::INTEGER) {
        read_sum<int>(index, data_ptr, out_data, col, limit, sum_val);
      } else {
        read_sum<float>(index, data_ptr, out_data, col, limit, sum_val);
      }
      col++;
      count = limit;
    } else if (fname  == "avg") {
      float sum_val = agg_context->groups_sum[g];
      int count_val = agg_context->groups_count[g];

      output.data[col].Initialize(true, limit);
      float* out_data = (float*)output.data[col].GetData();

      string sum_agg_key = agg_context->sub_aggs[0];
      string count_agg_key = agg_context->sub_aggs[1];
      
      int* count_ptr = (int*)fnode->alloc_vars[count_agg_key][0];
      void* data_ptr = fnode->alloc_vars[sum_agg_key][0];
      if (sub_aggs[ sum_agg_key ]->return_type == LogicalType::INTEGER) {
        read_avg<int>(index, data_ptr, count_ptr, out_data, col, limit, sum_val, count_val);
      } else {
        read_avg<float>(index, data_ptr, count_ptr, out_data, col, limit, sum_val, count_val);
      }
      col++;
      count = limit;
    } else if (fname == "stddev") {
      //  sum(x^2)/n - sum(x)^2/n^2
      //  (sum(x^2) - sum(x_remove^2))/(n - n_remove) - (sum(x) - sum(x_remove))^2/(n - n_remove)^2
      float sum_2_val = agg_context->groups_sum_2[g];
      float sum_val = agg_context->groups_sum[g];
      int count_val = agg_context->groups_count[g];
      
      output.data[col].Initialize(true, limit);
      float* out_data = (float*)output.data[col].GetData();

      string sum_agg_key = agg_context->sub_aggs[0];
      string sum_2_agg_key = agg_context->sub_aggs[1];
      string count_agg_key = agg_context->sub_aggs[2];

      void* sum_ptr = fnode->alloc_vars[sum_agg_key][0];
      float* sum_2_ptr = (float*)fnode->alloc_vars[sum_2_agg_key][0]; 
      int* count_ptr = (int*)fnode->alloc_vars[count_agg_key][0];
      if (sub_aggs[ sum_agg_key ]->return_type == LogicalType::INTEGER) {
        read_stddev<int>(index, sum_ptr, sum_2_ptr, count_ptr, out_data, col, limit, sum_val, sum_2_val, count_val);
      } else {
        read_stddev<float>(index, sum_ptr, sum_2_ptr, count_ptr, out_data, col, limit, sum_val, sum_2_val, count_val);
      }
      col++;
      count = limit;
    }

  }
  
  output.data[0].Sequence(data.offset, 1, count);

  data.offset += count;
	output.SetCardinality(count);
}

unique_ptr<FunctionData> FadeReaderFunction::FadeReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<FadeReaderBindData>();
  if (FadeState::cached_fade_result == nullptr) return std::move(result);
  auto &fnode = FadeState::cached_fade_result;
  
  idx_t out_var = 0;  // 1) get out_var idx using agg_id
  auto &lop_info =LineageState::qid_plans[fnode->qid][fnode->opid];
  
  names.emplace_back("pid");
  return_types.emplace_back(LogicalType::INTEGER);
  auto &agg_context = lop_info->agg_info->aggs[out_var];
  
  LogicalType &ret_type = agg_context->return_type;
  result->out_var = out_var;
  result->fname = agg_context->name;
  result->n_interventions = fnode->n_interventions;
  result->n_groups = 0;

  if (!fnode->groups.empty()) {
    for (int i=0; i < fnode->groups.size(); ++i) {
      idx_t g = fnode->groups[i];
      if (g >= lop_info->n_output) continue;
      result->n_groups++;
      names.emplace_back("g" + to_string(i));
      return_types.emplace_back(ret_type);
    }
  } else {
    result->n_groups = lop_info->n_output;
    for (int i=0; i < lop_info->n_output; ++i) {
      names.emplace_back("g" + to_string(i));
      return_types.emplace_back(ret_type);
    }
  }

  return std::move(result);
}

unique_ptr<LocalTableFunctionState>
FadeReaderFunction::FadeReaderInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
    GlobalTableFunctionState *gstate_p) {
  return make_uniq<FadeReaderLocalState>();
}

unique_ptr<GlobalTableFunctionState> FadeReaderFunction::FadeReaderInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    return make_uniq<FadeReaderGlobalState>();
}

unique_ptr<TableRef> FadeReaderFunction::FadeReaderReplacement(ClientContext &context, ReplacementScanInput &input,
    optional_ptr<ReplacementScanData> data) {
  auto table_name = ReplacementScan::GetFullPath(input);

  if (!ReplacementScan::CanReplace(table_name, {"fade_reader"})) {
    return nullptr;
  }
  
  auto table_function = make_uniq<TableFunctionRef>();
  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
  table_function->function = make_uniq<FunctionExpression>("fade_reader", std::move(children));

  if (!FileSystem::HasGlob(table_name)) {
    auto &fs = FileSystem::GetFileSystem(context);
    table_function->alias = fs.ExtractBaseName(table_name);
  }

  return std::move(table_function);
}

unique_ptr<NodeStatistics> FadeReaderFunction::Cardinality(ClientContext &context, const FunctionData *bind_data) {
  auto &data = bind_data->CastNoConst<FadeReaderBindData>();
  return make_uniq<NodeStatistics>(10);
}
unique_ptr<BaseStatistics> FadeReaderFunction::ScanStats(ClientContext &context,
    const FunctionData *bind_data_p, column_t column_index) {
  auto &bind_data = bind_data_p->CastNoConst<FadeReaderBindData>();
  auto stats = NumericStats::CreateUnknown(LogicalType::ROW_TYPE);
  NumericStats::SetMin(stats, Value::BIGINT(0));
  NumericStats::SetMax(stats, Value::BIGINT(10));
  stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES); // depends on the type of operator
  return stats.ToUnique();
}

TableFunctionSet FadeReaderFunction::GetFunctionSet() {
  // table_name/query_name: VARCHAR, lineage_ids: List(INT)
  TableFunction table_function("fade_reader", {LogicalType::INTEGER}, FadeReaderImplementation,
      FadeReaderBind, FadeReaderInitGlobal, FadeReaderInitLocal);

  table_function.statistics = ScanStats;
  table_function.cardinality = Cardinality;
  table_function.projection_pushdown = true;
  table_function.filter_pushdown = false;
  table_function.filter_prune = false;
  return MultiFileReader::CreateFunctionSet(table_function);
}
  
} // namespace duckdb
