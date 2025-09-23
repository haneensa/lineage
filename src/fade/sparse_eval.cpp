#include <iostream>

#include "fade/fade.hpp"
#include "lineage/lineage_init.hpp"

namespace duckdb {

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

  if (FadeState::debug)
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
      FadeState::cached_spec_stack.push_back(spec_key);
      if (FadeState::debug)
      std::cout <<  " base " << spec_key << " "<< n_input << " " << base_n_interventions << std::endl; 
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
    
    fnode.n_output = lineage.size();

    // TODO: if aggid is provided, then only compute result for it
    for (auto &sub_agg : FadeState::sub_aggs[qid_opid]) {
      auto &typ = sub_agg.second->return_type;
      idx_t col_idx = sub_agg.second->payload_idx;
      string& func = sub_agg.second->name;
      string out_key = sub_agg.first;
      if (FadeState::debug)
        std::cout << "[DEBUG]" << out_key << " " << func << " " << col_idx << " " << oids.size() << std::endl;
      for (int g=0; g < oids.size(); ++g) {
        int gid = oids[g].GetValue<int>();
        sparse_incremental_bw(func, g, lineage[gid], in_ann,
                              fnode.alloc_typ_vars[out_key].second[thread_id],
                              FadeState::input_data_map[qid_opid],
                              fnode.n_interventions, col_idx);
      }
    }
    return opid;
   } default: {}}

  return 100000;
}

void WhatIfSparse(ClientContext& context, int qid, int aggid,
                  unordered_map<string, vector<string>>& spec_map,
                  vector<Value>& oids) {
  if (LineageState::qid_plans_roots.find(qid) == LineageState::qid_plans_roots.end()) return;
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;
  
  unordered_map<idx_t, FadeNode> fade_data;
  idx_t root_id = LineageState::qid_plans_roots[qid];

	start_time = std::chrono::steady_clock::now();

  // unique per spec
  // 1.a traverse query plan, allocate fade nodes, and any memory allocation
  PrepareSparseFade(qid, root_id, aggid, fade_data, spec_map);
  // 1.b holds post interventions output. n_output X n_interventions per worker
  PrepareAggsNodes(qid, root_id, aggid, fade_data);

  reorder_between_root_and_agg(qid, root_id, oids);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double post_processing_time = time_span.count();
  std::cout << "end post process " << post_processing_time <<std::endl;
  

  // 3. Evaluate Interventions
  // 3.1 TODO: use workers
	start_time = std::chrono::steady_clock::now();
  InterventionSparse(qid, root_id, aggid, 0, fade_data, spec_map, oids);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double eval_time = time_span.count();
  std::cout << "end eval " << eval_time <<std::endl;

  int output_opid = get_output_opid(qid, root_id);
  if (FadeState::debug) std::cout << "output opid: " << output_opid << std::endl;

  if (output_opid < 0) return;
 
  auto& fnode = fade_data[output_opid];

  // 4. store output in global storage to be accessed later by the user
  FadeState::fade_results[qid] = {(idx_t)output_opid, fnode.n_output,
    fnode.n_interventions, std::move(oids),
    std::move(fnode.alloc_typ_vars)
  };
}
} // namespace duckdb
