#include <iostream>

#include "fade/fade.hpp"
#include "lineage/lineage_init.hpp"

namespace duckdb {

pair<int, int> get_start_end(int row_count, int thread_id, int num_worker) {
	int batch_size = row_count / num_worker;
	if (row_count % num_worker > 0) batch_size++;
	int start = thread_id * batch_size;
	int end   = start + batch_size;
	if (end >= row_count)  end = row_count;
	return std::make_pair(start, end);
}

// lineage: 1D backward lineage
// var: sparse input intervention matrix
// out: sparse output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
int whatif_sparse_filter(const vector<idx_t>& lineage, const vector<idx_t>& var,
                         vector<idx_t> out, int start, int end) {
	for (int i=start; i < end; ++i) {
		out[i] = var[lineage[i]];
	}
	return 0;
}

// lhs/rhs_lineage: 1D backward lineage
// lhs/rhs_var: sparse input intervention matrix
// out: sparse output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
// left/right_n_interventions: how many annotations the left/right side has
int whatif_sparse_join(const vector<idx_t>& lhs_lineage, const vector<idx_t>& rhs_lineage,
                       const vector<idx_t>& lhs_var,  const vector<idx_t>& rhs_var,
                       vector<idx_t>& out, const int start, const int end,
                       int left_n_interventions, int right_n_interventions) {
	if (left_n_interventions > 1 && right_n_interventions > 1) {
		for (int i=start; i < end; i++) {
			out[i] = lhs_var[lhs_lineage[i]] * right_n_interventions + rhs_var[rhs_lineage[i]];
		}
	} else if (left_n_interventions > 1) {
		for (int i=start; i < end; i++) {
			out[i] = lhs_var[lhs_lineage[i]];
		}
	} else {
		for (int i=start; i < end; i++) {
			out[i] = rhs_var[rhs_lineage[i]];
		}
	}

	return 0;
}

template <typename Tag>
int sparse_incremental_bw(const int g, const vector<idx_t>& bw_lineage, const vector<idx_t>&var_0,
                          void* __restrict__  out,
                          unordered_map<int, pair<LogicalType, void*>>& input_data_map,
                          int n_interventions, int col_idx);

struct CountTag {};
struct SumTag {};
struct Sum2Tag {};

// TODO: add logic to support registering new aggregation function
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


idx_t InterventionSparse(int qid, idx_t opid, idx_t agg_idx, idx_t thread_id,
    unordered_map<idx_t, FadeNode>& fade_data,
    unordered_map<string, vector<string>>& spec_map,
    vector<Value>& oids) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
    idx_t cid = InterventionSparse(qid, child, agg_idx,  thread_id, fade_data, spec_map, oids);
    children_opid.emplace_back(cid);
  }

  string qid_opid = to_string(qid) + "_" + to_string(opid);
  auto& fnode = fade_data[opid];

  if (LineageState::debug)
    std::cout << "tid: " << thread_id << " ISparse: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << 
      " " << qid_opid << ", n_output " << lop_info->n_output <<  " " << fnode.n_interventions << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
    auto& in_ann = fnode.annotations;
    idx_t n_input = in_ann.size();
    idx_t base_n_interventions = 1;
    for (const string  &col : spec_map[lop_info->table_name]) {
      // for each annotations, combine them a[0] + a[1] + .. + a[n]
      string spec_key = lop_info->table_name + "." + col;
      std::cout <<  lop_info->table_name << " base " << col << " "<< n_input << std::endl; 
      assert(FadeState::table_col_annotations[lop_info->table_name][col].size() == n_input);
      idx_t n_unique = FadeState::col_n_unique[spec_key];
      // TODO: do this once
      if (base_n_interventions == 1) {
        for (idx_t i=0; i < n_input; ++i) {
          in_ann[i] = FadeState::table_col_annotations[lop_info->table_name][col][i]; 
        }
      } else {
        for (idx_t i=0; i < n_input; ++i) {
          in_ann[i] = in_ann[i] * n_unique +
                      FadeState::table_col_annotations[lop_info->table_name][col][i]; 
        }
      }
      base_n_interventions *= n_unique;
    }
    return opid;
   } case LogicalOperatorType::LOGICAL_ORDER_BY: {
   } case LogicalOperatorType::LOGICAL_PROJECTION: {
     return children_opid[0];
   } case LogicalOperatorType::LOGICAL_FILTER: {
    auto& in_ann =  fade_data[children_opid[0]].annotations;
    auto& out_ann = fnode.annotations;
    if (out_ann.empty()) return children_opid[0];
    vector<idx_t>& lineage = LineageState::lineage_global_store[qid_opid][0];
    idx_t lineage_size = lineage.size();
    assert(lop_info->n_output == lineage_size);
		pair<int, int> start_end = get_start_end(lineage_size, thread_id, fnode.num_worker);
    if (FadeState::debug)
      std::cout << "[DEBUG] " << start_end.first << "  "<< start_end.second  << " " << lineage_size << std::endl;
    whatif_sparse_filter(lineage, in_ann, out_ann, start_end.first, start_end.second);
    // use cur_node->mtx to notify workers we are done
    return opid;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
    auto& out_ann = fnode.annotations;
    if (out_ann.empty()) return opid;
    auto& lhs_ann =  fade_data[children_opid[0]].annotations;
    auto& rhs_ann =  fade_data[children_opid[1]].annotations;
     
    vector<idx_t>& lhs_lineage = LineageState::lineage_global_store[qid_opid][0];
    vector<idx_t>& rhs_lineage = LineageState::lineage_global_store[qid_opid][1];
    assert(lhs_lineage.size() == rhs_lineage.size());
    idx_t lineage_size = lhs_lineage.size();
    assert(lop_info->n_output == lineage_size);

    idx_t lhs_n = fade_data[children_opid[0]].n_interventions;
    idx_t rhs_n = fade_data[children_opid[1]].n_interventions;
    if (FadeState::debug) std::cout << "[DEBUG] " <<  lhs_n << " " << rhs_n << std::endl;
    pair<int, int> start_end = get_start_end(lineage_size, thread_id, fnode.num_worker);
    whatif_sparse_join(lhs_lineage, rhs_lineage, lhs_ann, rhs_ann, out_ann,
          start_end.first, start_end.second, lhs_n, rhs_n);
     // use cur_node->mtx to notify workers we are done
    return opid;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    // if no interventions or number of threads per node is less than global number
    auto& in_ann =  fade_data[children_opid[0]].annotations;
    if (in_ann.empty()) return opid;
    vector<vector<idx_t>>& lineage = LineageState::lineage_global_store[qid_opid];
    //for (int i = 0; i < n_input; ++i) std::cout << " " <<  annotations_ptr[i];
    //std::cout << std::endl;
    // TODO: if aggid is provided, then only compute result for it
    for (auto &sub_agg : FadeState::sub_aggs[qid_opid]) {
      auto &typ = sub_agg.second->return_type;
      idx_t col_idx = sub_agg.second->payload_idx;
      string& func = sub_agg.second->name;
      string out_key = sub_agg.first;
    //  if (FadeState::debug)
        std::cout << "[DEBUG]" << out_key << " " << func << " " << col_idx << " " << oids.size() << std::endl;
      for (int g=0; g < oids.size(); ++g) {
        int gid = oids[g].GetValue<int>();
        std::cout << gid << " " << g << std::endl;
        sparse_incremental_bw(func, g, lineage[gid], in_ann,
                              fnode.alloc_vars[out_key][thread_id],
                              FadeState::input_data_map[qid_opid],
                              fnode.n_interventions, col_idx);
        std::cout << " done " << std::endl; 
      }
    }
    return opid;
   } default: {}}

  return 100000;
}

int get_output_opid(int query_id, idx_t operator_id) {
  auto &lop_info = LineageState::qid_plans[query_id][operator_id];
  if (lop_info->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    return operator_id;
  }

  for (auto &child : lop_info->children) {
    auto child_opid = get_output_opid(query_id, child);
    if (child_opid >= 0) return child_opid;
  }

  return -1;

}

void WhatIfSparse(ClientContext& context, int qid, int aggid,
                  unordered_map<string, vector<string>>& spec_map,
                  vector<Value>& oids) {
  if (LineageState::qid_plans_roots.find(qid) == LineageState::qid_plans_roots.end()) return;
  
  unordered_map<idx_t, FadeNode> fade_data;
  idx_t root_id = LineageState::qid_plans_roots[qid];

  // 1.a traverse query plan, allocate fade nodes, and any memory allocation
  PrepareSparseFade(qid, root_id, aggid, fade_data, spec_map);
  // 1.b holds post interventions output. n_output X n_interventions per worker
  PrepareAggsNodes(qid, root_id, aggid, fade_data, spec_map);
  
  // 3. Evaluate Interventions
  // 3.1 TODO: use workers
  InterventionSparse(qid, root_id, aggid, 0, fade_data, spec_map, oids);
  int output_opid = get_output_opid(qid, root_id);
  std::cout << "output opid: " << output_opid << std::endl;
 
  /* 
  // 4. store output in global storage to be accessed later by the user
  FadeState::cached_fade_result = std::move(fade_data[output_opid]);
  std::cout << "done" << std::endl;
  */
}
} // namespace duckdb
