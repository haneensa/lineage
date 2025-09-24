#include "fade/fade.hpp"
#include <iostream>
#include <cmath>


namespace duckdb {

idx_t MASK_SIZE = 16;

// ReadInterventions2D()
// -> give projection list, run them and construct the intervention matrix

// Dense allocate
idx_t PrepareDenseFade(idx_t qid, idx_t opid, idx_t agg_idx, idx_t n_interventions,
                      unordered_map<idx_t, FadeNode>& fade_data,
                      unordered_map<string, vector<string>>& spec_map) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
   idx_t cid = PrepareDenseFade(qid, child, agg_idx, n_interventions, fade_data, spec_map);
   children_opid.emplace_back(cid);
  }

  string qid_opid = to_string(qid) + "_" + to_string(opid);
  auto& fnode = fade_data[opid];
  fnode.num_worker = 1;
  fnode.n_interventions = 0;

  if (FadeState::debug)
    std::cout << opid <<  " dense prep fade:" << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
       << ", n_output:" << lop_info->n_output << ", n_interventions: " << n_interventions <<
       ", T: " << lop_info->table_name << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      // Only this and AGGs need to be rerun as specs change 
      auto spec_iter = spec_map.find(lop_info->table_name);
      if (spec_iter == spec_map.end()) return opid;
      idx_t n_input = FadeState::table_count[lop_info->table_name];
      fnode.n_interventions = n_interventions; // 
		  idx_t n_masks = std::ceil(fnode.n_interventions / MASK_SIZE);
			fnode.target_matrix = new Mask16[n_input * n_masks];
      std::fill(fnode.target_matrix, fnode.target_matrix + n_input*n_masks, 0);
      //std::fill(fnode.target_matrix, fnode.target_matrix + n_input*n_masks, 0xFFFF);
      std::cout << "PREP GET: " << n_input << " " << n_masks << " " << opid << std::endl;
		  /*for (int i=0; i < n_input*n_masks; i++) {
        if (fnode.target_matrix[i] > 0)
          std::cout << i << " " << fnode.target_matrix[i] << std::endl;
      }*/
      return opid;
   } case LogicalOperatorType::LOGICAL_FILTER: {
      fnode.n_interventions = fade_data[children_opid[0]].n_interventions;
		  idx_t n_masks = std::ceil(fnode.n_interventions / MASK_SIZE);
      if (!lop_info->has_lineage || fnode.n_interventions < 1) return children_opid[0];
			fnode.target_matrix = new Mask16[lop_info->n_output * n_masks];
      return opid;
   } case LogicalOperatorType::LOGICAL_ORDER_BY: {
   } case LogicalOperatorType::LOGICAL_PROJECTION: {
     return children_opid[0];
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      idx_t lhs_n = fade_data[children_opid[0]].n_interventions;
      idx_t rhs_n = fade_data[children_opid[1]].n_interventions;
      fnode.n_interventions = (lhs_n > 0) ? lhs_n : rhs_n;
      if (FadeState::debug) std::cout << "LHS_N. " << lhs_n << " RHS_N. " << rhs_n
                            << " " << fnode.n_interventions << std::endl;
      if (fnode.n_interventions > 0) {
		    idx_t n_masks = std::ceil(fnode.n_interventions / MASK_SIZE);
			  fnode.target_matrix = new Mask16[lop_info->n_output * n_masks];
      }
      return opid;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      return opid;
   } default: {}}

  return 10000;
}

// lineage: 1D backward lineage
// var: dense input intervention matrix
// out: dense output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
int whatif_filter(const vector<idx_t>& lineage, const Mask16* var,
                  Mask16* out, int start, int end, const idx_t n_masks) {
  std::cout << "whatif filter" << std::endl;
	for (idx_t i=start; i < end; ++i) {
    idx_t col_out = i * n_masks;
    idx_t col_oid = lineage[i] * n_masks;
    for (idx_t j=0; j < n_masks; ++j) {
		  out[col_out + j] = var[col_oid + j];
    }
	}
	return 0;
}

// lhs/rhs_lineage: 1D backward lineage
// lhs/rhs_var: dense input intervention matrix
// out: dense output intervention matrix
// start: the start partition to work on
// end: the end partition to work on
// left/right_n_interventions: how many annotations the left/right side has
int whatif_join(const vector<idx_t>& lhs_lineage, const vector<idx_t>& rhs_lineage,
                const Mask16* lhs_var,  const Mask16* rhs_var,
                Mask16* out, const int start, const int end,
                int left_n_interventions, int right_n_interventions,
                const idx_t n_masks) {
  std::cout << "whatif join: " << start << " " << end << " " <<
    left_n_interventions << " " << right_n_interventions << " " << n_masks << std::endl;
	if (left_n_interventions > 0 && right_n_interventions > 0) {
		for (int i=start; i < end; i++) {
      idx_t lhs_col = lhs_lineage[i] * n_masks;
      idx_t rhs_col = rhs_lineage[i] * n_masks;
      idx_t col = i*n_masks;
      for (idx_t j=0; j < n_masks; ++j) {
        out[col + j] = lhs_var[lhs_col + j] | rhs_var[rhs_col + j];
      }
		}
	} else if (left_n_interventions > 0) {
		for (int i=start; i < end; i++) {
      idx_t lhs_col = lhs_lineage[i] * n_masks;
      idx_t col = i*n_masks;
      for (idx_t j=0; j < n_masks; ++j) {
        out[col + j] = lhs_var[lhs_col + j];
      }
		}
	} else {
		for (int i=start; i < end; i++) {
      idx_t rhs_col = rhs_lineage[i] * n_masks;
      idx_t col = i*n_masks;
      for (idx_t j=0; j < n_masks; ++j) {
        out[col + j] = rhs_var[rhs_col + j];
       /* if (out[col+j] > 0)
          std::cout << "join " << i << " " << col << " " << j << " " << rhs_col << " " << out[col+j] << std::endl;
          */
      }
		}
	}

	return 0;
}

int whatif_agg_bw(const int g, const vector<idx_t>& bw_lineage, const Mask16* var_0,
                  void* __restrict__  out,
                  int n_interventions, int col_idx, const idx_t n_masks) {
//  std::cout << "whatif agg" << std::endl;
	int col = g * n_interventions;
  int* __restrict__ out_int = (int*)out;
  for (int i=0; i < bw_lineage.size(); ++i) {
    int iid = bw_lineage[i];
   // std::cout << "1 whatif agg " << i << " " << iid <<  std::endl;
    for (int j=0; j < n_masks; ++j) {
      int row = j * MASK_SIZE;
      Mask16 tmp_mask = ~var_0[iid * n_masks + j];
     // std::cout << "2 whatif agg " << j << " " << row <<  std::endl;
      for (int k=0; k < MASK_SIZE; ++k) {
      //  std::cout << "3 whatif agg " << k << std::endl;
      /*  if ((1 & tmp_mask >> k) == 1)
          std::cout << i << " " << k << " " << g << " " << col << " " << (1 & tmp_mask >> k) << std::endl;*/
        out_int[col + (row + k)] += (1 & tmp_mask >> k);
      }
    }
  }
 // std::cout << "4 whatif agg " << std::endl;
	return 0;
}

// Dense Evaluate
idx_t InterventionDense(int qid, idx_t opid, idx_t agg_idx, idx_t thread_id,
    unordered_map<idx_t, FadeNode>& fade_data, vector<Value>& oids) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
    idx_t cid = InterventionDense(qid, child, agg_idx,  thread_id, fade_data, oids);
    children_opid.emplace_back(cid);
  }

  string qid_opid = to_string(qid) + "_" + to_string(opid);
  auto& fnode = fade_data[opid];

  if (FadeState::debug)
    std::cout << "tid: " << thread_id << " IDense: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << 
      " " << qid_opid << ", n_output " << lop_info->n_output <<  " " << fnode.n_interventions << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
    Mask16* tm = fnode.target_matrix;
    // TODO: read intervention matrix
    idx_t n_input = FadeState::table_count[lop_info->table_name];
    idx_t n_masks = std::ceil(fnode.n_interventions / MASK_SIZE);
    std::cout << "GET: " << n_input << " " << n_masks << " " << opid << std::endl;
    /*for (int i=0; i < n_input*n_masks; i++) {
      if (tm[i] > 0)
        std::cout << "GET: " << i << " " << tm[i] << std::endl;
    }*/
    return opid;
   } case LogicalOperatorType::LOGICAL_ORDER_BY: {
   } case LogicalOperatorType::LOGICAL_PROJECTION: {
     return children_opid[0];
   } case LogicalOperatorType::LOGICAL_FILTER: {
    if (!lop_info->has_lineage || fnode.n_interventions == 0 ) return children_opid[0];
    Mask16* in_tm =  fade_data[children_opid[0]].target_matrix;
    Mask16* out_tm = fnode.target_matrix;
    vector<idx_t>& lineage = LineageState::lineage_global_store[qid_opid][0];
    idx_t lineage_size = lineage.size();
    assert(lop_info->n_output == lineage_size);
		pair<int, int> start_end = get_start_end(lineage_size, thread_id, fnode.num_worker);
		idx_t n_masks = std::ceil(fnode.n_interventions / MASK_SIZE);
    if (FadeState::debug)
      std::cout << "[DEBUG] " << start_end.first << "  "<< start_end.second  << " " << lineage_size << std::endl;

    whatif_filter(lineage, in_tm, out_tm, start_end.first, start_end.second, n_masks);

    return opid;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
    if (fnode.n_interventions == 0 ) return children_opid[0];
    Mask16* out_tm = fnode.target_matrix;
    Mask16* lhs_tm =  fade_data[children_opid[0]].target_matrix;
    Mask16* rhs_tm =  fade_data[children_opid[1]].target_matrix;
     
    vector<idx_t>& lhs_lineage = LineageState::lineage_global_store[qid_opid][0];
    vector<idx_t>& rhs_lineage = LineageState::lineage_global_store[qid_opid][1];
    assert(lhs_lineage.size() == rhs_lineage.size());
    idx_t lineage_size = lhs_lineage.size();
    assert(lop_info->n_output == lineage_size);

    idx_t lhs_n = fade_data[children_opid[0]].n_interventions;
    idx_t rhs_n = fade_data[children_opid[1]].n_interventions;
		idx_t n_masks = std::ceil(fnode.n_interventions / MASK_SIZE);
    if (FadeState::debug) std::cout << "[DEBUG] " <<  lhs_n << " " << rhs_n << " "
      << children_opid[0] << " " << children_opid[1] << std::endl;
    pair<int, int> start_end = get_start_end(lineage_size, thread_id, fnode.num_worker);
    /*for (int i=0; i < lineage_size*n_masks; i++) {
      if (rhs_tm[i] > 0)
        std::cout << "JOIN: " << i << " " << rhs_tm[i] << std::endl;
    }*/
    whatif_join(lhs_lineage, rhs_lineage, lhs_tm, rhs_tm, out_tm,
         start_end.first, start_end.second, lhs_n, rhs_n, n_masks);
    return opid;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    // if no interventions or number of threads per node is less than global number
    // TODO: replace with dense
    Mask16* in_ann =  fade_data[children_opid[0]].target_matrix;
    vector<vector<idx_t>>& lineage = LineageState::lineage_global_store[qid_opid];
		idx_t n_masks = std::ceil(fnode.n_interventions / MASK_SIZE);
    fnode.n_output = lineage.size();

    for (auto &sub_agg : FadeState::sub_aggs[qid_opid]) {
      auto &typ = sub_agg.second->return_type;
      idx_t col_idx = sub_agg.second->payload_idx;
      string& func = sub_agg.second->name;
      string out_key = sub_agg.first;
      if (FadeState::debug)
        std::cout << "[DEBUG]" << children_opid[0] << " " << out_key
          << " " << func << " " << col_idx << " " << oids.size() << std::endl;
      for (int g=0; g < oids.size(); ++g) {
        int gid = oids[g].GetValue<int>();
        whatif_agg_bw(g, lineage[gid], in_ann,
                  fnode.alloc_typ_vars[out_key].second[thread_id],
                  fnode.n_interventions, col_idx, n_masks);
      }
    }
    return opid;
   } default: {}}

  return 100000;
}


void WhatIfDense(ClientContext& context, int qid, int aggid,
                 unordered_map<string, vector<string>>& spec_map,
                 vector<Value>& oids) {
  if (LineageState::qid_plans_roots.find(qid) == LineageState::qid_plans_roots.end()) return;
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;
  
  unordered_map<idx_t, FadeNode> fade_data;
  idx_t root_id = LineageState::qid_plans_roots[qid];

  idx_t n_interventions = 16; // TODO: get this from user input

	start_time = std::chrono::steady_clock::now();

  // 1.a traverse query plan, allocate fade nodes, and any memory allocation
  PrepareDenseFade(qid, root_id, aggid, n_interventions, fade_data, spec_map);
  // 1.b holds post interventions output. n_output X n_interventions per worker
  PrepareAggsNodes(qid, root_id, aggid, fade_data);

  reorder_between_root_and_agg(qid, root_id, oids);
	end_time = std::chrono::steady_clock::now();
	time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);
	double post_processing_time = time_span.count();
  std::cout << "end post process " << post_processing_time <<std::endl;
  
  // evaluate
	start_time = std::chrono::steady_clock::now();
  InterventionDense(qid, root_id, aggid, 0, fade_data, oids);
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

}
