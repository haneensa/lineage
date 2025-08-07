#include "lineage/lineage_init.hpp"
#include "fade/fade.hpp"

#include <iostream>
#include <string>

namespace duckdb {

void InitGlobalLineage(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
    InitGlobalLineage(qid, child);
  }

  if (!lop_info->has_lineage) return;
  
  string table = to_string(qid) + "_" + to_string(opid);
  if (LineageState::debug)  std::cout << ">> GetCachedLineage: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
                                   << " table: " << table << std::endl;
  switch (lop_info->type) {
   case LogicalOperatorType::LOGICAL_ORDER_BY:
   case LogicalOperatorType::LOGICAL_FILTER: {
      vector<std::pair<Vector, int>>& chunked_lineage = LineageState::lineage_store[table];
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      glineage.emplace_back();
      std::cout << "chunked_lineage: " << chunked_lineage.size() << std::endl;
      for (auto& lin_n :chunked_lineage) {
        int64_t* col = reinterpret_cast<int64_t*>(lin_n.first.GetData());
        idx_t count = lin_n.second;
        std::cout << "count " << count << std::endl;
        for (idx_t i = 0; i < count; i++) glineage[0].emplace_back(col[i]);
      }
      if (LineageState::debug) std::cout << " |glineage[0]|: " << glineage[0].size() << std::endl;
     break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      glineage.resize(2);
      string join_table = table;
      for (idx_t i=0; i < 2; ++i) {
        if (i == 1) join_table += "_right";
        vector<std::pair<Vector, int>>& chunked_lineage = LineageState::lineage_store[join_table];
        for (auto& lin_n :chunked_lineage) {
          int64_t* col = reinterpret_cast<int64_t*>(lin_n.first.GetData());
          idx_t count = lin_n.second;
          for (idx_t k = 0; k < count; ++k)  glineage[i].emplace_back(col[k]);
        }
      }
      if (LineageState::debug) std::cout << " |glineage[0]|: " << glineage[0].size() << " |glineage[1]|: "
                                     << glineage[1].size() << std::endl;
     break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      // for each chunk, iterate over the values
      vector<std::pair<Vector, int>>& chunked_lineage = LineageState::lineage_store[table];
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      for (auto& lin_n :chunked_lineage) {
        idx_t count = lin_n.second;
        auto &list_vec = ListVector::GetEntry(lin_n.first); // child vector
        auto child_data = FlatVector::GetData<int64_t>(list_vec); // underlying int data
        auto list_data = FlatVector::GetData<list_entry_t>(lin_n.first); // offsets and lengths
        for (idx_t i = 0; i < count; i++) {
            auto entry = list_data[i];
            glineage.emplace_back();
            glineage.back().resize(entry.length);
            for (idx_t j = 0; j < entry.length; j++) {
              glineage.back()[j] = child_data[entry.offset + j];
              // lop_info->lineage1D[child_data[entry.offset+j]] = i;
            }
        }
      }
      if (LineageState::debug) std::cout << " |glineage|: " << glineage.size() << std::endl;
     break;
   } default: {}}
}

/*
void InitCardinalities(idx_t qid, idx_t opid) {}
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
   populate_and_verify_n_input_output(qid, child);
  }
  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      idx_t n_input = FadeState::table_count[lop_info->table_name];
      lop_info->n_input = n_input;
      lop_info->n_output = n_input;
      break;
   } case LogicalOperatorType::LOGICAL_FILTER: {
      lop_info->n_input = LineageState::qid_plans[qid][ lop_info->children[0] ]->n_output;
      break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      lop_info->n_input = LineageState::qid_plans[qid][ lop_info->children[0] ]->n_output;
      break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      // set by the cache operator. just need to verify
      idx_t child_n_output = LineageState::qid_plans[qid][ lop_info->children[0] ]->n_output;
      assert(lop_info->n_input == child_n_output);
      break;
		} default: {}
   }

  if (LineageState::debug)
    std::cout << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << " n_input: "
      << lop_info->n_input << ", n_output: " << lop_info->n_output << std::endl;
*/

// iterate over referenced aggregations by a query
  /*
void InitAggInfo(unique_ptr<AggInfo>& info,
                vector<unique_ptr<Expression>>& aggs,
                vector<LogicalType>& payload_types) {}
  if (LineageState::debug) std::cout << "get_agg_info: " << info->n_groups_attr << " " << aggs.size() << std::endl;
  int include_count = false;
  idx_t count_idx = aggs.size();
  // -1 excluding the lineage capture function
  for (idx_t i=0;  i < aggs.size()-1; ++i) {
    auto &agg_expr = aggs[i]->Cast<BoundAggregateExpression>();
    string name = agg_expr.function.name;
    if (LineageState::debug) std::cout << i << " agg: " << name << std::endl;
		if (include_count == false && (name == "count" || name == "count_star")) {
			include_count = true;
      count_idx = i;
			continue;
		} else if (name == "avg") {
			include_count = true;
		}
		
    if (name == "sum_no_overflow") name = "sum";
		
    if (name == "sum" || name == "avg" || name == "stddev") {
      D_ASSERT(agg_expr.children.size() > 1);
      D_ASSERT(agg_expr.children[0]->type == ExpressionType::BOUND_REF);
      auto &bound_ref_expr = agg_expr.children[0]->Cast<BoundReferenceExpression>();
			int col_idx = bound_ref_expr.index; 
      LogicalType ret_typ = LogicalType::FLOAT;
      LogicalType default_typ = LogicalType::FLOAT;
      if (payload_types[col_idx] == LogicalType::INTEGER ||
          payload_types[col_idx] == LogicalType::BIGINT) {
           default_typ = LogicalType::INTEGER;
           ret_typ = LogicalType::INTEGER;
      }
      info->payload_data.push_back({col_idx, default_typ});

      string sum_func_key = "sum_" + to_string(i);
      vector<string> sub_aggs_list = {sum_func_key};
      info->sub_aggs[sum_func_key] = make_uniq<SubAggsContext>("sum", default_typ, col_idx, i);

      if (name == "stddev") {
        string sum_2_func_key = "sum_2_" + to_string(i);
        info->sub_aggs[sum_2_func_key] = make_uniq<SubAggsContext>("sum_2", LogicalType::FLOAT, col_idx, i);
        sub_aggs_list.push_back(sum_2_func_key);
        ret_typ = LogicalType::FLOAT;
      }
      if (name == "avg" || name == "stddev") {
        sub_aggs_list.push_back("count");
        include_count = true;
        ret_typ = LogicalType::FLOAT;
      }
      info->aggs[i] = make_uniq<AggFuncContext>(name, ret_typ, std::move(sub_aggs_list));
    }
  }

  if (include_count) {
    info->sub_aggs["count"] = make_uniq<SubAggsContext>("count", LogicalType::INTEGER, 0, 0);
    vector<string> sub_aggs_list = {"count"};
    info->aggs[count_idx] = make_uniq<AggFuncContext>("count", LogicalType::INTEGER,
                                              std::move(sub_aggs_list));
  }
  
  // TODO: check if this has another child aggregate. set: has_agg_child and child_agg_id
  */

}
