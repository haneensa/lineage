#include "lineage/lineage_init.hpp"
#include "fade/fade.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

#include <iostream>
#include <string>

namespace duckdb {

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

void reorder_between_root_and_agg(idx_t qid, idx_t opid, vector<Value>& oids) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  
  if (lop_info->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
    // iterate over bw, replace groups[i] = bw[ groups[i] ];
    string qid_opid = to_string(qid) + "_" + to_string(opid);
    vector<idx_t>& lineage = LineageState::lineage_global_store[qid_opid][0];
    if (FadeState::debug)
      std::cout << qid_opid << " AdjustOutputIds order by " << lineage.size() << std::endl;
    for (idx_t i=0; i < oids.size(); ++i) {
      int gid = oids[i].GetValue<int>();
      oids[i] = Value::INTEGER( lineage[ gid ]);
    }
    return;
  } 
  
  if (lop_info->children.empty()) { return; }

  return reorder_between_root_and_agg(qid, lop_info->children[0], oids);
}

pair<int, int> get_start_end(int row_count, int thread_id, int num_worker) {
	int batch_size = row_count / num_worker;
	if (row_count % num_worker > 0) batch_size++;
	int start = thread_id * batch_size;
	int end   = start + batch_size;
	if (end >= row_count)  end = row_count;
	return std::make_pair(start, end);
}

void PartitionedLineage::fill_list_lineage(vector<vector<idx_t>>& glineage) {
  idx_t total_groups = this->get_total_count();
  glineage.resize(total_groups);
  idx_t p_id = 0;
  idx_t total_cnt = 0;
  for (auto &partition : left) {
    if (LineageState::debug)  std::cout << "chunked_lineage: " << partition.size() << std::endl;
    idx_t cur = 0;
    idx_t offset = 0;
    for (auto& entry :partition) {
      if (!entry.data || entry.count == 0) continue;
      idx_t p_offset = 0;
      if (!zones.empty()) {
        p_offset = zones[p_id][cur];
        // std::cout << "p_id: " << p_id << " cur: " << cur << " p : " << p_offset << " " << offset << std::endl;
      }
      auto list_entries = reinterpret_cast<const list_entry_t *>(entry.data);
      // The child data starts after all list_entry_t structures
      const idx_t entry_bytes = sizeof(list_entry_t) * entry.count;
      auto child_values = reinterpret_cast<const int64_t *>(entry.data + entry_bytes);
      ValidityMask mask;
      if (!entry.is_valid && entry.validity) {
          mask = ValidityMask(entry.validity, entry.count);
      }
      for (idx_t i = 0; i < entry.count; i++) {
          if (!entry.is_valid && entry.validity && !mask.RowIsValid(i)) continue;
          const auto &e = list_entries[i];
          glineage[i+p_offset].resize(e.length);
          for (idx_t j = 0; j < e.length; j++) {
            glineage[i+p_offset][j] = child_values[e.offset + j];
          }
      }
      offset += entry.count;
      cur++;
    }
    total_cnt += offset;
    p_id++;
  }
}

void PartitionedLineage::fill_local(vector<vector<IVector>>& store, vector<idx_t>& lineage1D) {
  idx_t p_id = 0;
  idx_t offset = 0;
  for (auto &partition : store) {
    if (LineageState::debug)
      std::cout << "chunked_lineage: " << partition.size() << std::endl;
    idx_t cur = 0;
    for (auto& lin_n :partition) {
      idx_t count = lin_n.count;
      if (!lin_n.is_valid && !lin_n.data) {
        // TODO: need to mark it with null
        if (zones.empty() || zones[p_id].empty()) {
          for (idx_t k = 0; k < count; ++k)  lineage1D[k+offset] = 0;
        } else {
          idx_t partition_offset = zones[p_id][cur];
          for (idx_t k = 0; k < count; ++k)  {
            lineage1D[k+partition_offset] = 0;
          }
        }
      } else {
        int64_t* col = reinterpret_cast<int64_t*>(lin_n.data);
        if (zones.empty() || zones[p_id].empty()) {
          for (idx_t k = 0; k < count; ++k)  {
            lineage1D[k+offset] = col[k];
          }
        } else {
          idx_t partition_offset = zones[p_id][cur];
        //  std::cout << p_id << " " << cur << "partition_offset: " << partition_offset << " " << col[0] << " " << std::endl;
          for (idx_t k = 0; k < count; ++k)  {
            lineage1D[k+partition_offset] = col[k];
          }
        }
      }
      cur++;
      offset += count;
    }
    p_id++;
  }
}

idx_t PartitionedLineage::get_total_count() {
  idx_t total_count = 0;
  for (auto &partition : local_offsets) {
    for (auto& cnt : partition) {
      total_count += cnt;
    }
  }
  return total_count;
}
void PartitionedLineage::fill_lineage(bool is_left, vector<idx_t>& lineage1D, idx_t total_count) {
  lineage1D.resize(total_count);

  if (is_left)  this->fill_local(left, lineage1D);
  else this->fill_local(right, lineage1D);
  if (LineageState::debug) {
    for (idx_t i=0; i < lineage1D.size(); ++i) {
      std::cout << i << " " << lineage1D[i] << std::endl;
    }
  }
}

idx_t InitGlobalLineageBuff(ClientContext& context, idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  string table = to_string(qid) + "_" + to_string(opid);
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
    idx_t cid = InitGlobalLineageBuff(context, qid, child);
    children_opid.push_back(cid);
  }

 if (LineageState::debug)
    std::cout << ">> GetCachedLineage: " <<
                      EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
                      << " table: " << table << std::endl;
  switch (lop_info->type) {
   case LogicalOperatorType::LOGICAL_PROJECTION:
   case LogicalOperatorType::LOGICAL_TOP_N:
   case LogicalOperatorType::LOGICAL_ORDER_BY:
   case LogicalOperatorType::LOGICAL_FILTER: {
      if (!lop_info->has_lineage) return children_opid[0];
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      glineage.emplace_back();
      idx_t total_count = LineageState::partitioned_store_buf[table]->get_total_count();
      LineageState::partitioned_store_buf[table]->fill_lineage(true, glineage[0], total_count);
      lop_info->n_output = glineage[0].size();
      if (LineageState::debug)
        std::cout << "filter/order/topn: |glineage[0]|: " << glineage[0].size() << " " << total_count << std::endl;
      return opid;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      glineage.resize(2);
      idx_t total_count = LineageState::partitioned_store_buf[table]->get_total_count();
      for (idx_t i=0; i < 2; ++i) {
        if (i == 1) {
          LineageState::partitioned_store_buf[table]->fill_lineage(false, glineage[1], total_count);
        } else {
          LineageState::partitioned_store_buf[table]->fill_lineage(true, glineage[0], total_count);
        }
      }
      lop_info->n_output = glineage[0].size();
      if (LineageState::debug)
        std::cout << " |glineage[0]|: " << glineage[0].size() << " |glineage[1]|: "
                                     << glineage[1].size() << " " << total_count << std::endl;
      return opid;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      idx_t total_count = 0;
      if (!LineageState::logs.empty()) {
        unordered_map<int64_t, idx_t> finalize_codes;
        unordered_map<int64_t, idx_t> combine_codes;
        idx_t nlogs = LineageState::logs.size();
        idx_t groups_cnt = 0;
        for (idx_t tid=0; tid < nlogs; ++tid) {
          string table = to_string(opid) + "_" + to_string(tid);
          if (LineageState::logs.find(table) == LineageState::logs.end()) continue;
          auto& arts = LineageState::logs[table];
          for (auto& e : arts->buffer_agg_finalize_log) {
            if (LineageState::debug) PrintLoggedVector(e.first, 8);
            int64_t* col = reinterpret_cast<int64_t*>(e.first.data);
            for (idx_t i = 0; i < e.first.count; i++) {
              if (finalize_codes.find(col[i]) != finalize_codes.end()) {
                std::cout << finalize_codes[col[i]] << " dublicate finalize " << col[i] << " table " << table << std::endl;
              }
              finalize_codes[col[i]] = i + e.second;
              groups_cnt = std::max(groups_cnt, i+e.second);
            }
          }
          arts->buffer_agg_finalize_log.clear();
        }
        
          
        groups_cnt += 1;
        glineage.resize(groups_cnt);
        for (idx_t tid=0; tid < nlogs; ++tid) {
          string table = to_string(opid) + "_" + to_string(tid);
          if (LineageState::logs.find(table) == LineageState::logs.end()) continue;
          auto& arts = LineageState::logs[table];
          for (auto& e : arts->buffer_agg_combine_log) {
              int64_t* col_src = reinterpret_cast<int64_t*>(e.first.data);
              int64_t* col_target = reinterpret_cast<int64_t*>(e.second.data);
              for (idx_t i = 0; i < e.first.count; i++) {
                if (finalize_codes.find(col_target[i]) == finalize_codes.end()) {
                  std::cout << " end of finalize " << std::endl;
                }
                if (combine_codes.find(col_src[i]) != combine_codes.end()) {
                  std::cout << " dublicates " << std::endl;
                }
                combine_codes[col_src[i]] = finalize_codes[col_target[i]];
              }
          }
          arts->buffer_agg_combine_log.clear();
        }
        if (combine_codes.empty()) {
          combine_codes = std::move(finalize_codes);
          finalize_codes = {};
        }
        finalize_codes.clear();
          
        for (idx_t tid=0; tid < nlogs; ++tid) {
          string table = to_string(opid) + "_" + to_string(tid);
          if (LineageState::logs.find(table) == LineageState::logs.end()) continue;
          auto& arts = LineageState::logs[table];
          for (auto& e : arts->buffer_agg_update_log) {
            int64_t* col = reinterpret_cast<int64_t*>(e.first.data);
            int64_t* col_idx = reinterpret_cast<int64_t*>(e.second[0].data); // assert
            for (idx_t i = 0; i < e.first.count; i++) {
              idx_t group_id = combine_codes[col[i]];
              col[i] = group_id; // forward lineage
              glineage[group_id].emplace_back(col_idx[i]);
            }
            total_count += e.first.count;
          }
        }
      } else if (LineageState::partitioned_store_buf.find(table) != 
          LineageState::partitioned_store_buf.end()) {
        LineageState::partitioned_store_buf[table]->fill_list_lineage(glineage);
      }
      lop_info->n_output = glineage.size();
      if (LineageState::debug) {
          std::cout << "total_count: " << total_count << std::endl;
          std::cout << " |glineage|: " << glineage.size() << std::endl;
      }

      return opid;
   } default: {}}
  
  return 10000000;
}



idx_t InitGlobalLineage(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  string table = to_string(qid) + "_" + to_string(opid);
  if (LineageState::debug)  std::cout << ">> GetCachedLineage: " <<
                      EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
                      << " table: " << table << std::endl;
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
    idx_t cid = InitGlobalLineage(qid, child);
    children_opid.push_back(cid);
  }

  switch (lop_info->type) {
   case LogicalOperatorType::LOGICAL_PROJECTION:
   case LogicalOperatorType::LOGICAL_TOP_N:
   case LogicalOperatorType::LOGICAL_ORDER_BY:
   case LogicalOperatorType::LOGICAL_FILTER: {
      if (!lop_info->has_lineage) return children_opid[0];
      vector<std::pair<Vector, int>>& chunked_lineage = LineageState::lineage_store[table];
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      glineage.emplace_back();
      if (LineageState::debug)
        std::cout << "chunked_lineage: " << chunked_lineage.size() << std::endl;
      for (auto& lin_n :chunked_lineage) {
        int64_t* col = reinterpret_cast<int64_t*>(lin_n.first.GetData());
        idx_t count = lin_n.second;
        if (FadeState::debug) std::cout << "count " << count << std::endl;
        for (idx_t i = 0; i < count; i++) glineage[0].emplace_back(col[i]);
      }
      lop_info->n_output = glineage[0].size();
      if (LineageState::debug) std::cout << "filter/order/topn: |glineage[0]|: " << glineage[0].size() << std::endl;
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
      if (LineageState::debug) std::cout << " |glineage[0]|: " << glineage[0].size() << " |glineage[1]|: "
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
      if (LineageState::debug) std::cout << " |glineage|: " << glineage.size() << std::endl;
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
  }
  if (name == "count" || name == "count_star") {
    vector<string> sub_aggs_list = {"count"};
    FadeState::aggs[qid_opid][key] = make_shared_ptr<AggFuncContext>("count", LogicalType::INTEGER, std::move(sub_aggs_list));
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
    ExtractAggsContext(qid_opid, aggs[i], payload_types, i);
  }
}

void CastDecimalToFloat(Vector& new_vec, string qid_opid, idx_t count, LogicalType typ, idx_t col_idx, idx_t i) {
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
	    Vector new_vec(LogicalType::FLOAT, count);
      CastDecimalToFloat(new_vec, qid_opid, count, typ, col_idx, i);
      col = reinterpret_cast<T1*>(new_vec.GetData());
      for (idx_t i=0; i < count; ++i) {
        input_values[i+offset] = col[i];
      }
		} else {
      for (idx_t i=0; i < count; ++i) {
        input_values[i+offset] = col[i];
      }
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
    if (FadeState::debug)
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
	fnode.alloc_typ_vars[out_var].second[t] = malloc(sizeof(T) * n_output * n_interventions);
	memset(fnode.alloc_typ_vars[out_var].second[t], 0, sizeof(T) * n_output * n_interventions);
}


idx_t PrepareAggsNodes(idx_t qid, idx_t opid, idx_t agg_idx,
                      unordered_map<idx_t, FadeNode>& fade_data) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
   idx_t cid = PrepareAggsNodes(qid, child, agg_idx, fade_data);
   children_opid.emplace_back(cid);
  }

  string qid_opid = to_string(qid) + "_" + to_string(opid);
  auto& fnode = fade_data[opid];

  if (FadeState::debug)
    std::cout << "prep aggs fade:" << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
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
      std::cout << "n_interventions: " << fnode.n_interventions << std::endl;
      // alloc per worker t. n_output X n_interventions
      // TODO: access FadeState::aggs and only evaluate its sub_aggs
      for (auto &sub_agg : FadeState::sub_aggs[qid_opid]) {
        auto &typ = sub_agg.second->return_type;
        idx_t col_idx = sub_agg.second->payload_idx;
        string& func = sub_agg.second->name;
        string out_key = sub_agg.first;
        fnode.alloc_typ_vars[out_key].second.assign(fnode.num_worker, 0);
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
