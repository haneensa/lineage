#include "lineage/lineage_init.hpp"
#include "fade/fade_reader.hpp"

#include <iostream>
#include <string>

namespace duckdb {

// TODO: add logic to support registering new aggregation function
struct CountTag {};
struct SumTag {};
struct Sum2Tag {};
struct AvgTag {};
struct StdvTag {};


/* incremental aggrergates with sparse target matrix */

template <typename Tag>
int sparse_incremental_bw(const int g, const vector<idx_t>& bw_lineage, const vector<idx_t>&var_0,
                          void* __restrict__  out,
                          unordered_map<int, pair<LogicalType, void*>>& input_data_map,
                          int n_interventions, int col_idx);

template <>
int sparse_incremental_bw<CountTag>(const int g, const vector<idx_t>& bw_lineage, const vector<idx_t>&var_0,
                                void* __restrict__  out,
                                unordered_map<int, pair<LogicalType, void*>>& input_data_map,
                                int n_interventions, int col_idx) {
	int col = g * n_interventions;
  int* __restrict__ out_int = (int*)out;
  for (int i=0; i < bw_lineage.size(); ++i) {
    int iid = bw_lineage[i];
    int row = var_0[iid];
    out_int[col + row]++;
  }
	return 0;
}

template <>
int sparse_incremental_bw<SumTag>(const int g, const vector<idx_t>& bw_lineage, const vector<idx_t>&var_0,
                                void* __restrict__  out,
                                unordered_map<int, pair<LogicalType, void*>>& input_data_map,
                                int n_interventions, int col_idx) {
	int col = g * n_interventions;
  if (input_data_map[col_idx].first == LogicalType::INTEGER) {
    int* __restrict__  out_int = (int*)out;
    int *in_arr = reinterpret_cast<int *>(input_data_map[col_idx].second);
    for (int i=0; i < bw_lineage.size(); ++i) {
      int iid = bw_lineage[i];
    //  std::cout << i << " " << iid << " " << var_0.size() << std::endl;
      int row = var_0[iid];
    //  std::cout << " post 1 " << row << std::endl;
      out_int[col + row] +=  in_arr[iid];
    //  std::cout << " post " << std::endl;
    }
  } else {
    float* __restrict__  out_int = (float*)out;
    float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx].second);
    for (int i=0; i < bw_lineage.size(); ++i) {
      int iid = bw_lineage[i];
   //   std::cout << i << " " << iid << " " << var_0.size() << std::endl;
      int row = var_0[iid];
     // std::cout << " post 1 " << row << std::endl;
      out_int[col + row] +=  in_arr[iid];
     // std::cout << " post " << std::endl;
    }
  }
	return 0;
} 

template <>
int sparse_incremental_bw<Sum2Tag>(const int g, const vector<idx_t>& bw_lineage, const vector<idx_t>&var_0,
                                void* __restrict__  out,
                                unordered_map<int, pair<LogicalType, void*>>& input_data_map,
                                int n_interventions, int col_idx) {
  float* __restrict__  out_float = (float*)out;
	int col = g * n_interventions;
  if (input_data_map[col_idx].first == LogicalType::INTEGER) {
    int *in_arr = reinterpret_cast<int *>(input_data_map[col_idx].second);
    for (int i=0; i < bw_lineage.size(); ++i) {
      int iid = bw_lineage[i];
      int row = var_0[iid];
      out_float[col + row] += (in_arr[iid] * in_arr[iid]);
    }
  } else {
    float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx].second);
    for (int i=0; i < bw_lineage.size(); ++i) {
      int iid = bw_lineage[i];
      int row = var_0[iid];
      out_float[col + row] += (in_arr[iid] * in_arr[iid]);
    }
  }
	return 0;
}

void sparse_incremental_bw(string func, const int g, const vector<idx_t>& bw_lineage, const vector<idx_t>&var_0,
                          void* __restrict__  out,
                          unordered_map<int, pair<LogicalType, void*>>& input_data_map,
                          int n_interventions, int col_idx) {
  if (func == "sum") {
    sparse_incremental_bw<SumTag>(g, bw_lineage, var_0, out, input_data_map,  n_interventions, col_idx);
  } else if (func == "count") {
    sparse_incremental_bw<CountTag>(g, bw_lineage, var_0, out, input_data_map, n_interventions, col_idx);
  } else if (func == "sum_2") {
    sparse_incremental_bw<Sum2Tag>(g, bw_lineage, var_0, out, input_data_map, n_interventions, col_idx);
  }
}

/* deserialize aggregation output */

template<class T>
void read_sum(idx_t index, void* in_data, void* out_data, idx_t col, idx_t limit, idx_t sum_val) {
  T* data_ptr = (T*)in_data;
  T* col_data = (T*)out_data;
  for (int i = 0; i < limit; ++i) {
    col_data[i] = sum_val-data_ptr[index + i];
  }
}

template<class T>
void read_avg(idx_t index, void* in_data, int* count_ptr, float* col_data, idx_t col, idx_t limit,
              idx_t sum_val, idx_t count_val) {
  T* data_ptr = (T*)in_data;
  for (int i = 0; i < limit; ++i) {
    // TODO: make sure we don't divide by 0
    col_data[i] = (sum_val - data_ptr[index + i]) / (count_val - count_ptr[index + i]);
  }
}

template<class T>
void read_stddev(idx_t index, void* in_data, float* sum_2_ptr, int* count_ptr, float* col_data, idx_t col, idx_t limit,
              idx_t sum_val, idx_t sum_2_val, idx_t count_val) {
  T* sum_ptr = (T*)in_data;
  for (int i = 0; i < limit; ++i) {
    // TODO: make sure we don't divide by 0
    float sum_base = sum_val - sum_ptr[index + i];
    int count_base = count_val - count_ptr[index + i];
    col_data[i] = std::sqrt( (sum_2_val - sum_2_ptr[index + i]) / count_base - (sum_base*sum_base) / (count_base*count_base) );
  }
}

template <typename Tag>
void IReader(string qid_opid, idx_t out_var, FadeResult& fnode,
            DataChunk& output, idx_t index, idx_t row, idx_t col, idx_t count);


template <>
void IReader<CountTag>(string qid_opid, idx_t out_var, FadeResult& fnode,
                       DataChunk& output, idx_t index, idx_t row, idx_t col, idx_t count) {
  auto &agg_context = FadeState::aggs[qid_opid][out_var];
  int* col_data = (int*)output.data[col].GetData();
  string count_agg_key = agg_context->sub_aggs[0];
  string total_count = "total_" + count_agg_key;
  int* count_vals = ((int*)fnode.alloc_typ_vars[total_count].second[0]);
  int count_val = count_vals[row];
  int* count_ptr = (int*)fnode.alloc_typ_vars[count_agg_key].second[0];
  
  if (FadeState::debug)
  std::cout << "count: " <<  qid_opid << " " << row << "  aggs => " << total_count << " " << count_val << std::endl;

  for (int v = 0; v < count; ++v) {
    col_data[v] = count_val-count_ptr[index + v];
  }
}

template<>
void IReader<SumTag>(string qid_opid, idx_t out_var, FadeResult& fnode,
                       DataChunk& output, idx_t index, idx_t row, idx_t col, idx_t count) {
  auto &agg_context = FadeState::aggs[qid_opid][out_var];
  void* out_data = output.data[col].GetData();
  string sum_agg_key = agg_context->sub_aggs[0];
  void* data_ptr = fnode.alloc_typ_vars[sum_agg_key].second[0];
  string total_sum = "total_" + sum_agg_key;
  float sum_val = ((float*)fnode.alloc_typ_vars[total_sum].second[0])[row];
  
  if (FadeState::debug)
  std::cout << "sum: " <<  qid_opid << " " << row << "  aggs => " << total_sum << " " << sum_val << std::endl;

  if (FadeState::sub_aggs[qid_opid][ sum_agg_key ]->return_type == LogicalType::INTEGER) {
    read_sum<int>(index, data_ptr, out_data, col, count, sum_val);
  } else {
    read_sum<float>(index, data_ptr, out_data, col, count, sum_val);
  }
}

template<>
void IReader<AvgTag>(string qid_opid, idx_t out_var, FadeResult& fnode,
                       DataChunk& output, idx_t index, idx_t row, idx_t col, idx_t count) {
  auto &agg_context = FadeState::aggs[qid_opid][out_var];

  float* out_data = (float*)output.data[col].GetData();

  string sum_agg_key = agg_context->sub_aggs[0];
  string count_agg_key = agg_context->sub_aggs[1];
  
  string total_sum = "total_" + sum_agg_key;
  string total_count = "total_" + count_agg_key;

  int count_val = ((int*)fnode.alloc_typ_vars[total_count].second[0])[row];
  float sum_val = ((float*)fnode.alloc_typ_vars[total_sum].second[0])[row];
  
  if (FadeState::debug)
  std::cout << "avg: " <<  qid_opid << " " << row << "  aggs => " << total_count << " " << sum_val << " " << count_val << std::endl;

  int* count_ptr = (int*)fnode.alloc_typ_vars[count_agg_key].second[0];
  void* data_ptr = fnode.alloc_typ_vars[sum_agg_key].second[0];
  if (FadeState::sub_aggs[qid_opid][ sum_agg_key ]->return_type == LogicalType::INTEGER) {
    read_avg<int>(index, data_ptr, count_ptr, out_data, col, count, sum_val, count_val);
  } else {
    read_avg<float>(index, data_ptr, count_ptr, out_data, col, count, sum_val, count_val);
  }
}

template<>
void IReader<StdvTag>(string qid_opid, idx_t out_var, FadeResult& fnode,
                       DataChunk& output, idx_t index, idx_t row, idx_t col, idx_t count) {
  auto &agg_context = FadeState::aggs[qid_opid][out_var];
  //  sum(x^2)/n - sum(x)^2/n^2
  //  (sum(x^2) - sum(x_remove^2))/(n - n_remove) - (sum(x) - sum(x_remove))^2/(n - n_remove)^2
  
  float* out_data = (float*)output.data[col].GetData();

  string sum_agg_key = agg_context->sub_aggs[0];
  string sum_2_agg_key = agg_context->sub_aggs[1];
  string count_agg_key = agg_context->sub_aggs[2];
  
  string total_sum = "total_" + sum_agg_key;
  string total_sum_2 = "total_" + sum_2_agg_key;
  string total_count = "total_" + count_agg_key;

  int count_val = ((int*)fnode.alloc_typ_vars[total_count].second[0])[row];
  float sum_2_val = ((float*)fnode.alloc_typ_vars[total_sum_2].second[0])[row];
  float sum_val = ((float*)fnode.alloc_typ_vars[total_sum].second[0])[row];

  void* sum_ptr = fnode.alloc_typ_vars[sum_agg_key].second[0];
  float* sum_2_ptr = (float*)fnode.alloc_typ_vars[sum_2_agg_key].second[0]; 
  int* count_ptr = (int*)fnode.alloc_typ_vars[count_agg_key].second[0];
  
  if (FadeState::debug)
  std::cout << "stddev: " <<  qid_opid << " " << row << "  aggs => " << total_count << " " << sum_val << " " << count_val << " " << sum_2_val << std::endl;
  
  if (FadeState::sub_aggs[qid_opid][ sum_agg_key ]->return_type == LogicalType::INTEGER) {
    read_stddev<int>(index, sum_ptr, sum_2_ptr, count_ptr, out_data, col, count, sum_val, sum_2_val, count_val);
  } else {
    read_stddev<float>(index, sum_ptr, sum_2_ptr, count_ptr, out_data, col, count, sum_val, sum_2_val, count_val);
  }
}

void IReader(string func, string qid_opid, FadeResult& fnode, idx_t out_var,
            DataChunk& output, idx_t index, idx_t row, idx_t col, idx_t count) {
  if (func == "sum") {
    IReader<SumTag>(qid_opid, out_var, fnode, output, index, row, col, count);
  } else if (func == "count") {
    IReader<CountTag>(qid_opid, out_var, fnode, output, index, row, col, count);
  } else if (func == "avg") {
    IReader<AvgTag>(qid_opid, out_var, fnode, output, index, row, col, count);
  } else if (func == "stddev") {
    IReader<StdvTag>(qid_opid, out_var, fnode, output, index, row, col, count);
  }
}



}
