namespace duckdb {
/*
// vars: mask_size (16)
// n_masks = n_intervention / mask_size,

// Dense allocate
// Dense Evaluate

// GenRandomWhatIfIntervention
// -> give projection list, run them and construtc the intervention matrix
// ReadInterventions2D()
void Intervention2DEval(int query_id, idx_t operator_id, idx_t thread_id,
    std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
    unordered_map<string, vector<string>>& spec_map, vector<int>& groups) {
  auto &lop_info = LineageState::qid_plans[query_id][operator_id];
  auto &fnode = fade_data[operator_id];
  string table_name = to_string(query_id) + "_" + to_string(lop_info->opid);
  
  for (auto &child : lop_info->children) {
    Intervention2DEvalquery_id, child, thread_id, fade_data, spec_map, groups);
  }

  if (LineageState::debug)
    std::cout << "tid: " << thread_id << " ISparse: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << 
      " " << table_name << ", n_input:" << lop_info->n_input << ", n_output" << lop_info->n_output << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
    break;
   } case LogicalOperatorType::LOGICAL_FILTER: {
    if (fnode->n_interventions <= 1) return;
    vector<int>& lineage = lop_info->lineage1D;;
    idx_t lineage_size = lineage.size();
    assert(lop_info->n_output == lineage_size);
		pair<int, int> start_end = get_start_end(lineage_size, thread_id, FadeState::num_worker);
    if (LineageState::debug)
      std::cout << "[DEBUG] " << start_end.first << "  "<< start_end.second  << " " << lineage_size << std::endl;
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
     break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    // if no interventions or number of threads per node is less than global number
    if (fnode->n_interventions <= 1 || thread_id >= fnode->num_worker) return;
    idx_t n_input = lop_info->n_input;
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
      if (groups.empty()) { // if number of groups requested is empty, use forward lineage
		    pair<int, int> start_end = get_start_end(n_input, thread_id, fnode->num_worker);
      } else {
        for (int g=0; g < groups.size(); ++g) {
          int gid = groups[g];
          if (gid >= lop_info->n_output) {
            if (FadeState::debug)
              std::cout << " Requested group out of range " << lop_info->n_output << " " << g << " " << gid << std::endl;
            continue;
          }
        }
      }
      if (LineageState::debug) std::cout << "done" << std::endl;
    }
     break;
   } default: {}}

}*/

}
