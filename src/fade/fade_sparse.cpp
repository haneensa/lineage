// TODO model fade as an operator in a query plan -- need to handle combining multile pipelines per table--:
//         leaf nodes are scan nodes for any table we want to intervene on.
//         add compressing projection, read only the attributes we care about. 
//         then parent is the fade node that takes this as input and start the internvention
//         Q. how to partition data and avoid materializing the whole input?

#include <iostream>

#include "fade_extension.hpp"
#include "fade/fade_node.hpp"
#include "lineage/lineage_init.hpp"

namespace duckdb {

// lineage: 1D backward lineage
// var: sparse input intervention matrix
// out: sparse output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
int whatif_sparse_filter(vector<int>& lineage,
                         int* __restrict__ var,
                         int* __restrict__ out,
                         int start, int end) {
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
int whatif_sparse_join(int* lhs_lineage, int* rhs_lineage,
                       int* __restrict__ lhs_var,  int* __restrict__ rhs_var,
                       int* __restrict__ out, const int start, const int end,
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

int groupby_agg_incremental_arr_single_group_bw(int g, vector<int>& bw_lineage, int* __restrict__ var_0,
                                void* __restrict__  out,
                                unordered_map<int, void*>& input_data_map,
                                unordered_map<int, LogicalType>& input_data_types_map,
                                int n_interventions, int col_idx, string& func, int limit) {
	int col = g * n_interventions;
	if (func == "count") {
		int* __restrict__ out_int = (int*)out;
		for (int i=0; i < bw_lineage.size(); ++i) {
			int iid = bw_lineage[i];
			int row = var_0[iid];
			out_int[col + row]++;
		}
	} else if (func == "sum") { // sum
    if (input_data_types_map[col_idx] == LogicalType::INTEGER) {
			int* __restrict__  out_int = (int*)out;
			int *in_arr = reinterpret_cast<int *>(input_data_map[col_idx]);
		  for (int i=0; i < bw_lineage.size(); ++i) {
			  int iid = bw_lineage[i];
				int row = var_0[iid];
				out_int[col + row] +=  in_arr[iid];
			}
		} else {
			float* __restrict__  out_float = (float*)out;
			float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
		  for (int i=0; i < bw_lineage.size(); ++i) {
			  int iid = bw_lineage[i];
				int row = var_0[iid];
				out_float[col + row] += in_arr[iid];
			}
		}
	} else if (func == "sum_2") {
      float* __restrict__  out_float = (float*)out;
      if (input_data_types_map[col_idx] == LogicalType::INTEGER) {
        int *in_arr = reinterpret_cast<int *>(input_data_map[col_idx]);
        for (int i=0; i < bw_lineage.size(); ++i) {
          int iid = bw_lineage[i];
          int row = var_0[iid];
          out_float[col + row] += (in_arr[iid] * in_arr[iid]);
        }
      } else {
        float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
        for (int i=0; i < bw_lineage.size(); ++i) {
          int iid = bw_lineage[i];
          int row = var_0[iid];
          out_float[col + row] += (in_arr[iid] * in_arr[iid]);
        }
      }
  }
	return 0;
}

int groupby_agg_incremental_arr_forward(vector<int>& lineage, int* __restrict__ var_0,
                                void* __restrict__  out,
                                std::unordered_map<int, void*>& input_data_map,
                                std::unordered_map<int, LogicalType>& input_data_types_map,
                                const int start, const int end,
                                int n_interventions, int col_idx, string func) {
	if (func == "count") {
		int* __restrict__ out_int = (int*)out;
		for (int i=start; i < end; ++i) {
			int oid = lineage[i];
			int col = oid * n_interventions;
			int row = var_0[i];
			out_int[col + row] += 1;
		}
	} else if (func == "sum") { // sum
    if (input_data_types_map[col_idx] == LogicalType::INTEGER) {
			int* __restrict__  out_int = (int*)out;
			int *in_arr = reinterpret_cast<int *>(input_data_map[col_idx]);
			for (int i=start; i < end; ++i) {
				int oid = lineage[i];
				int col = oid * n_interventions;
				int row = var_0[i];
				out_int[col + row] += in_arr[i];
			}
		} else {
			float* __restrict__  out_float = (float*)out;
			float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
			for (int i=start; i < end; ++i) {
				int oid = lineage[i];
				int col = oid * n_interventions;
				int row = var_0[i];
				out_float[col + row] += in_arr[i];
			}
		}
	} else if (func == "sum_2") {
			float* __restrict__  out_float = (float*)out;
      if (input_data_types_map[col_idx] == LogicalType::INTEGER) {
        int *in_arr = reinterpret_cast<int *>(input_data_map[col_idx]);
        for (int i=start; i < end; ++i) {
          int oid = lineage[i];
          int col = oid * n_interventions;
          int row = var_0[i];
          out_float[col + row] += (in_arr[i] * in_arr[i]);
        }
      } else {
        float *in_arr = reinterpret_cast<float *>(input_data_map[col_idx]);
        for (int i=start; i < end; ++i) {
          int oid = lineage[i];
          int col = oid * n_interventions;
          int row = var_0[i];
          out_float[col + row] += (in_arr[i] * in_arr[i]);
        }
      }
  }
	return 0;
}


void InterventionSparse(int query_id, idx_t operator_id, idx_t thread_id,
    std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
    unordered_map<string, vector<string>>& spec_map, vector<int>& groups) {
  auto &lop_info = LineageState::qid_plans[query_id][operator_id];
  auto &fnode = fade_data[operator_id];
  string table_name = to_string(query_id) + "_" + to_string(lop_info->opid);
  
  for (auto &child : lop_info->children) {
    InterventionSparse(query_id, child, thread_id, fade_data, spec_map, groups);
  }

  if (LineageState::debug)
    std::cout << "tid: " << thread_id << " ISparse: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << 
      " " << table_name << ", n_input:" << lop_info->n_input << ", n_output" << lop_info->n_output << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
    //TODO: combine annotations
    if (fnode->n_interventions <= 1) return;
    auto snode = dynamic_cast<FadeSparseNode*>(fnode.get());
    idx_t base_n_interventions = 1;
    for (const string  &col : spec_map[lop_info->table_name]) {
      // for each annotations, combine them a[0] + a[1] + .. + a[n]
      string spec_key = lop_info->table_name + "." + col;
      if (LineageState::debug)
        std::cout <<  lop_info->table_name << " base " << col << " "<< lop_info->n_input << std::endl; 
      assert(FadeState::table_col_annotations[lop_info->table_name][col].size() == lop_info->n_input);
      idx_t n_unique = FadeState::col_n_unique[spec_key];
      // TODO: do this once
      if (base_n_interventions == 1) {
        for (idx_t i=0; i < lop_info->n_input; ++i) {
          snode->annotations[i] = FadeState::table_col_annotations[lop_info->table_name][col][i]; 
        }
      } else {
        for (idx_t i=0; i < lop_info->n_input; ++i) {
          snode->annotations[i] = snode->annotations[i] * n_unique +
                                  FadeState::table_col_annotations[lop_info->table_name][col][i]; 
        }
      }
      base_n_interventions *= n_unique;
    }
    break;
   } case LogicalOperatorType::LOGICAL_FILTER: {
    if (fnode->n_interventions <= 1) return;
    vector<int>& lineage = lop_info->lineage1D;;
    idx_t lineage_size = lineage.size();
    assert(lop_info->n_output == lineage_size);
		pair<int, int> start_end = get_start_end(lineage_size, thread_id, FadeState::num_worker);
    if (LineageState::debug)
      std::cout << "[DEBUG] " << start_end.first << "  "<< start_end.second  << " " << lineage_size << std::endl;
    auto base_annotations = dynamic_cast<FadeSparseNode*>(fade_data[lop_info->children[0]].get())->annotations.get();
    auto annotations = dynamic_cast<FadeSparseNode*>(fnode.get())->annotations.get();
    whatif_sparse_filter(lineage, base_annotations, annotations, start_end.first, start_end.second);
    // use cur_node->mtx to notify workers we are done
     break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
     if (fnode->n_interventions <= 1) return;
     assert(lop_info->lineage2D.size() == 2);
     int lhs_n_interventions = fade_data[lop_info->children[0]]->n_interventions;
     int rhs_n_interventions = fade_data[lop_info->children[1]]->n_interventions;
     int* lhs_lineage = lop_info->lineage2D[0].data(); 
     int* rhs_lineage = lop_info->lineage2D[1].data();
     idx_t lineage_size = lop_info->lineage2D[0].size();
    if (LineageState::debug)
      std::cout << "[DEBUG] " <<  lhs_n_interventions << " " << rhs_n_interventions << std::endl;
     assert(lop_info->n_output == lineage_size);
     int* lhs_annotations = dynamic_cast<FadeSparseNode*>(fade_data[lop_info->children[0]].get())->annotations.get();
     int* rhs_annotations = dynamic_cast<FadeSparseNode*>(fade_data[lop_info->children[1]].get())->annotations.get();
     auto annotations = dynamic_cast<FadeSparseNode*>(fnode.get())->annotations.get();
     pair<int, int> start_end = get_start_end(lineage_size, thread_id, FadeState::num_worker);
     whatif_sparse_join(lhs_lineage, rhs_lineage, lhs_annotations, rhs_annotations, annotations,
          start_end.first, start_end.second, lhs_n_interventions, rhs_n_interventions);
     // use cur_node->mtx to notify workers we are done
     break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    // if no interventions or number of threads per node is less than global number
    if (fnode->n_interventions <= 1 || thread_id >= fnode->num_worker) return;
    idx_t n_input = lop_info->n_input;
    int* annotations_ptr = dynamic_cast<FadeSparseNode*>(fade_data[lop_info->children[0]].get())->annotations.get();
    assert(lop_info->n_input == lop_info->lineage1D.size());
    //for (int i = 0; i < n_input; ++i) std::cout << " " <<  annotations_ptr[i];
    //std::cout << std::endl;
    // TODO: if aggid is provided, then only compute result for it
    for (auto &sub_agg : lop_info->agg_info->sub_aggs) {
      auto &typ = sub_agg.second->return_type;
      idx_t col_idx = sub_agg.second->payload_idx;
      string& func = sub_agg.second->name;
      string out_key = sub_agg.first;
      if (LineageState::debug)
        std::cout << "[DEBUG]" << out_key << " " << func << " " << col_idx << " " << groups.size() << std::endl;
      /*if (lop_info->agg_info->input_data_types_map[col_idx] == LogicalType::INTEGER) {
        for (int i = 0; i < n_input; ++i) std::cout << func << " " << col_idx << " " << i << " " << ((int*)lop_info->agg_info->input_data_map[col_idx])[i] << std::endl;
      } else {
        for (int i = 0; i < n_input; ++i) std::cout << func << " " << col_idx << " " << i << " " << ((float*)lop_info->agg_info->input_data_map[col_idx])[i] << std::endl;
      }*/
      if (groups.empty()) { // if number of groups requested is empty, use forward lineage
		    pair<int, int> start_end = get_start_end(n_input, thread_id, fnode->num_worker);
        // sub_agg.second->incremental_forward();
        groupby_agg_incremental_arr_forward(lop_info->lineage1D, annotations_ptr,
            fnode->alloc_vars[out_key][thread_id],
            lop_info->agg_info->input_data_map,
            lop_info->agg_info->input_data_types_map,
            start_end.first, start_end.second,
            fnode->n_interventions, col_idx, func);
      } else {
        for (int g=0; g < groups.size(); ++g) {
          int gid = groups[g];
          if (gid >= lop_info->n_output) {
            std::cout << " Requested group out of range " << lop_info->n_output << " " << g << " " << gid << std::endl;
            continue;
          }
          
          // sub_agg.second->incremental_bw();
          groupby_agg_incremental_arr_single_group_bw(g, lop_info->lineage2D[gid],
                                      annotations_ptr,
                                      fnode->alloc_vars[out_key][thread_id],
                                      lop_info->agg_info->input_data_map,
                                      lop_info->agg_info->input_data_types_map,
                                      fnode->n_interventions,
                                      col_idx, func, n_input);
        }
      }
      if (LineageState::debug) std::cout << "done" << std::endl;
    }
     break;
   } default: {}}

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
                  vector<int>& groups) {
  if (LineageState::qid_plans_roots.find(qid) == LineageState::qid_plans_roots.end()) return;
  
  // 1. traverse query plan, allocate fade nodes, and any memory allocation, read input?
  std::unordered_map<idx_t, unique_ptr<FadeNode>> fade_data;
  // 2. decompose GetCachedData(); ?
  prepare_fade_plan(qid, LineageState::qid_plans_roots[qid], fade_data, spec_map);
  idx_t root_id = LineageState::qid_plans_roots[qid];
  
  // 3. Evaluate Interventions
  // 3.1 TODO: use workers
  InterventionSparse(qid, root_id, 0, fade_data, spec_map, groups);
  // 4. store output in global storage to be accessed later by the user
  int output_opid = get_output_opid(qid, root_id);
  FadeState::cached_fade_result = std::move(fade_data[output_opid]);
}

} // namespace duckdb
