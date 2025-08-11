#include "lineage/lineage_init.hpp"
#include "fade/fade.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

#include <iostream>
#include <string>

namespace duckdb {

idx_t InitGlobalLineage(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
    idx_t cid = InitGlobalLineage(qid, child);
    children_opid.push_back(cid);
  }

  string table = to_string(qid) + "_" + to_string(opid);
  if (FadeState::debug)  std::cout << ">> GetCachedLineage: " <<
                      EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
                      << " table: " << table << std::endl;
  switch (lop_info->type) {
   case LogicalOperatorType::LOGICAL_PROJECTION: {
      return children_opid[0];
   } case LogicalOperatorType::LOGICAL_ORDER_BY:
     case LogicalOperatorType::LOGICAL_FILTER: {
      if (!lop_info->has_lineage) return children_opid[0];
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
      lop_info->n_output = glineage[0].size();
      if (FadeState::debug) std::cout << " |glineage[0]|: " << glineage[0].size() << std::endl;
      return opid;
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
      lop_info->n_output = glineage[0].size();
      if (FadeState::debug) std::cout << " |glineage[0]|: " << glineage[0].size() << " |glineage[1]|: "
                                     << glineage[1].size() << std::endl;
      return opid;
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
      lop_info->n_output = glineage.size();
      if (FadeState::debug) std::cout << " |glineage|: " << glineage.size() << std::endl;
      return opid;
   } default: {}}
  
  return 10000000;
}

// use aggregate offset as key
void ExtractAggsContext(string qid_opid, unique_ptr<Expression>& expr,
                        vector<LogicalType>& payload_types, idx_t key) {
  auto &agg_expr = expr->Cast<BoundAggregateExpression>();
  string name = agg_expr.function.name;
  
  if (FadeState::debug) std::cout << key << " agg: " << name << std::endl;
  
  if (name == "sum_no_overflow") name = "sum";
  
  bool add_count = false;
  auto count_iter = FadeState::sub_aggs[qid_opid].find("count");
  if (count_iter == FadeState::sub_aggs[qid_opid].end()
      && (name == "count" || name == "count_star")) {
    add_count = true;
  } else if (name == "sum" || name == "avg" || name == "stddev") {
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

    FadeState::payload_data[qid_opid].push_back({col_idx, default_typ});

    string sum_func_key = "sum_" + to_string(col_idx);
    vector<string> sub_aggs_list = {sum_func_key};
    FadeState::sub_aggs[qid_opid][sum_func_key] = make_shared_ptr<SubAggsContext>("sum", default_typ, col_idx, key);

    if (name == "stddev") {
      string sum_2_func_key = "sum_2_" + to_string(col_idx);
      FadeState::sub_aggs[qid_opid][sum_2_func_key] = make_shared_ptr<SubAggsContext>("sum_2", LogicalType::FLOAT, col_idx, key);
      sub_aggs_list.push_back(sum_2_func_key);
      ret_typ = LogicalType::FLOAT;
    }
    
    if (name == "avg" || name == "stddev") {
      sub_aggs_list.push_back("count");
      add_count = true;
      ret_typ = LogicalType::FLOAT;
    }
    FadeState::aggs[qid_opid][key] = make_shared_ptr<AggFuncContext>(name, ret_typ, std::move(sub_aggs_list));
  }
  
  if (add_count) {
    FadeState::sub_aggs[qid_opid]["count"] = make_shared_ptr<SubAggsContext>("count", LogicalType::INTEGER, 0, 0);
    if (name == "count" || name == "count_star") {
      vector<string> sub_aggs_list = {"count"};
      FadeState::aggs[qid_opid][key] = make_shared_ptr<AggFuncContext>("count", LogicalType::INTEGER, std::move(sub_aggs_list));
    }
  }
}

// iterate over referenced aggregations by a query
void InitAggInfo(string qid_opid, vector<unique_ptr<Expression>>& aggs,
                vector<LogicalType>& payload_types) {
  if (FadeState::debug) std::cout << "InitAggInfo: " <<  aggs.size() << std::endl;
  int include_count = false;
  // -1 excluding the lineage capture function
  for (idx_t i=0;  i < aggs.size()-1; ++i) {
    auto &agg_expr = aggs[i]->Cast<BoundAggregateExpression>();
    if (agg_expr.function.name == "count" || agg_expr.function.name == "count_star") include_count = true;
    ExtractAggsContext(qid_opid, aggs[i], payload_types, i);
  }
  
  // none of the aggregates added. do we need it?
  auto count_iter = FadeState::sub_aggs[qid_opid].find("count");
  idx_t key = aggs.size();
  if (!include_count && count_iter != FadeState::sub_aggs[qid_opid].end()) {
    vector<string> sub_aggs_list = {"count"};
    FadeState::aggs[qid_opid][key] = make_shared_ptr<AggFuncContext>("count", LogicalType::INTEGER, std::move(sub_aggs_list));
  }
}

template<class T1>
T1* CastDecimalToFloat(string qid_opid, idx_t count, LogicalType typ, idx_t col_idx, idx_t i) {
	Vector new_vec(LogicalType::FLOAT, count);
  CastParameters parameters;
  uint8_t width = DecimalType::GetWidth(typ);
  uint8_t scale = DecimalType::GetScale(typ);
  switch (typ.InternalType()) {
  case PhysicalType::INT16: {
    VectorCastHelpers::TemplatedDecimalCast<int16_t, float, TryCastFromDecimal>(
        FadeState::cached_cols[qid_opid][col_idx][i], new_vec, count, parameters, width, scale);
    break;
  } case PhysicalType::INT32: {
    VectorCastHelpers::TemplatedDecimalCast<int32_t, float, TryCastFromDecimal>(
        FadeState::cached_cols[qid_opid][col_idx][i], new_vec, count, parameters, width, scale);
    break;
  } case PhysicalType::INT64: {
    VectorCastHelpers::TemplatedDecimalCast<int64_t, float, TryCastFromDecimal>(
        FadeState::cached_cols[qid_opid][col_idx][i], new_vec, count, parameters, width, scale);
    break;
  } case PhysicalType::INT128: {
    VectorCastHelpers::TemplatedDecimalCast<hugeint_t, float, TryCastFromDecimal>(
      FadeState::cached_cols[qid_opid][col_idx][i], new_vec, count, parameters, width, scale);
    break;
  } default: {
    throw InternalException("Unimplemented internal type for decimal");
  }
  }
	
  return  reinterpret_cast<T1*>(new_vec.GetData());
}

template<class T1, class T2>
T2* GetInputVals(string qid_opid, idx_t col_idx, idx_t row_count) {
  idx_t chunk_count = FadeState::cached_cols_sizes[qid_opid].size();
	T2* input_values = new T2[row_count];
  
  if (chunk_count == 0) return input_values;
	auto typ = FadeState::cached_cols[qid_opid][col_idx][0].GetType();
	idx_t offset = 0;
	for (idx_t i=0; i < chunk_count; ++i) {
		T1* col = reinterpret_cast<T1*>(FadeState::cached_cols[qid_opid][col_idx][i].GetData());
		int count = FadeState::cached_cols_sizes[qid_opid][i];
		if (typ.id() == LogicalTypeId::DECIMAL) {
      col = CastDecimalToFloat<T1>(qid_opid, count, typ, col_idx, i);
		}
    // TODO: handle null values
		for (idx_t i=0; i < count; ++i) {
			input_values[i+offset] = col[i];
		}
		offset +=  count;
	}

	return input_values;
}

void GetCachedVals(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
   GetCachedVals(qid, child);
  }

  if (lop_info->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    string qid_opid = to_string(qid) + "_" + to_string(opid);
    idx_t count = 0;
    for (auto& c : FadeState::cached_cols_sizes[qid_opid]) {
      count += c;
    }
    std::cout << qid_opid << " GetCachedVals " << count << " " << FadeState::cached_cols_sizes.size() <<  std::endl;
    for (auto& out_var : FadeState::payload_data[qid_opid]) {
      int col_idx = out_var.first;
      if (FadeState::cached_cols[qid_opid][col_idx].empty()) continue;
      //if (FadeState::debug) 
      auto in_typ = FadeState::cached_cols[qid_opid][col_idx][0].GetType();

      pair<LogicalType, void*>& p = FadeState::input_data_map[qid_opid][col_idx];
      p.first = in_typ;
      if (in_typ == LogicalType::INTEGER) {
        p.second = GetInputVals<int, int>(qid_opid, col_idx, count);
      } else if (in_typ == LogicalType::BIGINT) {
        p.first = LogicalType::INTEGER;
        p.second = GetInputVals<int64_t, int>(qid_opid, col_idx, count);
      } else if (in_typ == LogicalType::FLOAT) {
        p.second = GetInputVals<float, float>(qid_opid, col_idx, count);
      } else if (in_typ == LogicalType::DOUBLE) {
        p.first = LogicalType::FLOAT;
        p.second = GetInputVals<double, float>(qid_opid, col_idx, count);
      } else {
        p.first = LogicalType::FLOAT;
        p.second = GetInputVals<float, float>(qid_opid, col_idx, count);
      }
    }
  }
}

template<class T>
void allocate_agg_output(FadeNode& fnode, idx_t t, idx_t n_interventions,
    int n_output, string out_var) {
	fnode.alloc_vars[out_var][t] = aligned_alloc(64, sizeof(T) * n_output * n_interventions);
	if (fnode.alloc_vars[out_var][t] == nullptr) {
		fnode.alloc_vars[out_var][t] = malloc(sizeof(T) * n_output * n_interventions);
	}
	memset(fnode.alloc_vars[out_var][t], 0, sizeof(T) * n_output * n_interventions);
}


idx_t PrepareAggsNodes(idx_t qid, idx_t opid, idx_t agg_idx,
                      unordered_map<idx_t, FadeNode>& fade_data,
                      unordered_map<string, vector<string>>& spec_map) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
   idx_t cid = PrepareAggsNodes(qid, child, agg_idx, fade_data, spec_map);
   children_opid.emplace_back(cid);
  }

  string qid_opid = to_string(qid) + "_" + to_string(opid);
  auto& fnode = fade_data[opid];

  if (FadeState::debug)
    std::cout << "prep fade exit:" << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
       << ", n_output:" << lop_info->n_output << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      return opid;
   } case LogicalOperatorType::LOGICAL_FILTER: {
      if (!lop_info->has_lineage || fnode.n_interventions < 1) return children_opid[0];
      return opid;
   } case LogicalOperatorType::LOGICAL_ORDER_BY: {
   } case LogicalOperatorType::LOGICAL_PROJECTION: {
     return children_opid[0];
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      return opid;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      fnode.n_interventions = fade_data[children_opid[0]].n_interventions;
      int n_output = lop_info->n_output;
      // alloc per worker t. n_output X n_interventions
      // TODO: access FadeState::aggs and only evaluate its sub_aggs
      for (auto &sub_agg : FadeState::sub_aggs[qid_opid]) {
        auto &typ = sub_agg.second->return_type;
        idx_t col_idx = sub_agg.second->payload_idx;
        string& func = sub_agg.second->name;
        string out_key = sub_agg.first;
        fnode.alloc_vars[out_key].assign(fnode.num_worker, 0);
        for (int t=0; t < fnode.num_worker; ++t) {
          if (typ == LogicalType::INTEGER)
            allocate_agg_output<int>(fnode, t, fnode.n_interventions, n_output, out_key);
          else
            allocate_agg_output<float>(fnode, t, fnode.n_interventions, n_output, out_key);
        }
      }
      return opid;
   } default: {}}

  return 10000;
}



}
