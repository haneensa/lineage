namespace duckdb {

// vars: mask_size (16)
// n_masks = n_intervention / mask_size,

// Dense allocate
// Dense Evaluate

// GenRandomWhatIfIntervention
// -> give projection list, run them and construtc the intervention matrix
// ReadInterventions2D()

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
    std::cout << "tid: " << thread_id << " ISparse: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << 
      " " << qid_opid << ", n_output " << lop_info->n_output <<  " " << fnode.n_interventions << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
    auto& in_ann = fnode.annotations; // TODO; replace with dense
    idx_t n_input = in_ann.size();
    
    // TODO: read intervention matrix

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

    // TODO: replace with dense
    // whatif_sparse_filter(lineage, in_ann, out_ann, start_end.first, start_end.second);
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
    // TODO: replace with dense
    // whatif_sparse_join(lhs_lineage, rhs_lineage, lhs_ann, rhs_ann, out_ann,
    //     start_end.first, start_end.second, lhs_n, rhs_n);
    return opid;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    // if no interventions or number of threads per node is less than global number
    auto& in_ann =  fade_data[children_opid[0]].annotations;
    if (in_ann.empty()) return opid;
    vector<vector<idx_t>>& lineage = LineageState::lineage_global_store[qid_opid];
    
    fnode.n_output = lineage.size();

    for (auto &sub_agg : FadeState::sub_aggs[qid_opid]) {
      auto &typ = sub_agg.second->return_type;
      idx_t col_idx = sub_agg.second->payload_idx;
      string& func = sub_agg.second->name;
      string out_key = sub_agg.first;
      if (FadeState::debug)
        std::cout << "[DEBUG]" << out_key << " " << func << " " << col_idx << " " << oids.size() << std::endl;
      for (int g=0; g < oids.size(); ++g) {
        int gid = oids[g].GetValue<int>();
        // TODO: replace with dense
        /* sparse_incremental_bw(func, g, lineage[gid], in_ann,
                              fnode.alloc_typ_vars[out_key].second[thread_id],
                              FadeState::input_data_map[qid_opid],
                              fnode.n_interventions, col_idx); */
      }
    }
    return opid;
   } default: {}}

  return 100000;
}

idx_t PrepareDenseFade(idx_t qid, idx_t opid, idx_t agg_idx,
                      unordered_map<idx_t, FadeNode>& fade_data) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
   idx_t cid = PrepareSparseFade(qid, child, agg_idx, fade_data, spec_map);
   children_opid.emplace_back(cid);
  }

  string qid_opid = to_string(qid) + "_" + to_string(opid);
  auto& fnode = fade_data[opid];
  fnode.num_worker = 1;
  fnode.n_interventions = 0;

  if (FadeState::debug)
    std::cout << "prep fade exit:" << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
       << ", n_output:" << lop_info->n_output << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      // Only this and AGGs need to be rerun as specs change 
      auto spec_iter = spec_map.find(lop_info->table_name);
      if (spec_iter == spec_map.end()) return opid;
      fnode.n_interventions = 1;
      idx_t n_input = FadeState::table_count[lop_info->table_name];
      for (const string  &col : spec_iter->second) {
        string spec_key = lop_info->table_name + "." + col;
        fnode.n_interventions *= FadeState::col_n_unique[spec_key];
      }
      fnode.annotations.assign(n_input, 0); // TODO: if spec is single array, avoid this
      if (FadeState::debug)
        std::cout << "Table Name: " << lop_info->table_name
                <<  ", n_interventions: " << fnode.n_interventions << std::endl;
      return opid;
   } case LogicalOperatorType::LOGICAL_FILTER: {
      fnode.n_interventions = fade_data[children_opid[0]].n_interventions;
      if (!lop_info->has_lineage || fnode.n_interventions < 1) return children_opid[0];
      fnode.annotations.assign(lop_info->n_output, 0);
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
      if (fnode.n_interventions > 1) fnode.annotations.assign(lop_info->n_output, 0);
      return opid;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      return opid;
   } default: {}}

  return 10000;
}


void WhatIfDense(ClientContext& context, int qid, int aggid,
                 unordered_map<string, vector<string>>& spec_map,
                 vector<Value>& oids) {
  if (LineageState::qid_plans_roots.find(qid) == LineageState::qid_plans_roots.end()) return;
	std::chrono::steady_clock::time_point start_time, end_time;
	std::chrono::duration<double> time_span;
  
  unordered_map<idx_t, FadeDenseNode> fade_data;
  idx_t root_id = LineageState::qid_plans_roots[qid];

	start_time = std::chrono::steady_clock::now();

  // allocate del_interventions
  // 1.a traverse query plan, allocate fade nodes, and any memory allocation
  PrepareDenseFade(qid, root_id, aggid, fade_data, spec_map);
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
