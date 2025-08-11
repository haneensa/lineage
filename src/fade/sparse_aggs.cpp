#include "lineage/lineage_init.hpp"
#include "fade/fade.hpp"

#include <iostream>
#include <string>

namespace duckdb {

/*
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
template<>
void IReader<"count">(idx_t out_var, DataChunk& output, idx_t row, idx_t col, idx_t count) {
  auto &agg_context = FadeState::aggs[out_var];

  int count_val = agg_context->groups_count[row];
  
  int* col_data = (int*)output.data[col].GetData();
  
  string count_agg_key = agg_context->sub_aggs[0];

  int* count_ptr = (int*)fnode->alloc_vars[count_agg_key][0];
  for (int v = 0; v < count; ++v) {
    col_data[v] = count_val-count_ptr[index + v];
  }
}

template<>
void IReader<"sum">(idx_t out_var, DataChunk& output, idx_t row, idx_t col, idx_t count) {
  auto &agg_context = FadeState::aggs[out_var];

  float sum_val = agg_context->groups_sum[row];

  void* out_data = output.data[col].GetData();
  
  string sum_agg_key = agg_context->sub_aggs[0];

  void* data_ptr = fnode->alloc_vars[sum_agg_key][0];
  if (FadeState::sub_aggs[qid][ sum_agg_key ]->return_type == LogicalType::INTEGER) {
    read_sum<int>(index, data_ptr, out_data, col, count, sum_val);
  } else {
    read_sum<float>(index, data_ptr, out_data, col, count, sum_val);
  }
}

template<>
void IReader<"avg">(idx_t out_var, DataChunk& output, idx_t row, idx_t col, idx_t count) {
  auto &agg_context = FadeState::aggs[out_var];

  float sum_val = agg_context->groups_sum[row];
  int count_val = agg_context->groups_count[row];

  float* out_data = (float*)output.data[col].GetData();

  string sum_agg_key = agg_context->sub_aggs[0];
  string count_agg_key = agg_context->sub_aggs[1];
  
  int* count_ptr = (int*)fnode->alloc_vars[count_agg_key][0];
  void* data_ptr = fnode->alloc_vars[sum_agg_key][0];
  if (FadeState::sub_aggs[qid][ sum_agg_key ]->return_type == LogicalType::INTEGER) {
    read_avg<int>(index, data_ptr, count_ptr, out_data, col, count, sum_val, count_val);
  } else {
    read_avg<float>(index, data_ptr, count_ptr, out_data, col, count, sum_val, count_val);
  }
}

template<>
void IReader<"stdev">(idx_t out_var, DataChunk& output, idx_t row, idx_t col, idx_t count) {
  auto &agg_context = FadeState::aggs[out_var];
  //  sum(x^2)/n - sum(x)^2/n^2
  //  (sum(x^2) - sum(x_remove^2))/(n - n_remove) - (sum(x) - sum(x_remove))^2/(n - n_remove)^2
  float sum_2_val = agg_context->groups_sum_2[row];
  float sum_val = agg_context->groups_sum[row];
  int count_val = agg_context->groups_count[row];
  
  float* out_data = (float*)output.data[col].GetData();

  string sum_agg_key = agg_context->sub_aggs[0];
  string sum_2_agg_key = agg_context->sub_aggs[1];
  string count_agg_key = agg_context->sub_aggs[2];

  void* sum_ptr = fnode->alloc_vars[sum_agg_key][0];
  float* sum_2_ptr = (float*)fnode->alloc_vars[sum_2_agg_key][0]; 
  int* count_ptr = (int*)fnode->alloc_vars[count_agg_key][0];
  if (FadeState::sub_aggs[qid][ sum_agg_key ]->return_type == LogicalType::INTEGER) {
    read_stddev<int>(index, sum_ptr, sum_2_ptr, count_ptr, out_data, col, count, sum_val, sum_2_val, count_val);
  } else {
    read_stddev<float>(index, sum_ptr, sum_2_ptr, count_ptr, out_data, col, count, sum_val, sum_2_val, count_val);
  }
}*/


}
