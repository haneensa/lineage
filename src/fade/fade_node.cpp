#include "fade/fade_node.hpp"

#include <iostream>
#include <fstream>

#include "fade_extension.hpp"
#include "lineage/lineage_init.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

bool reorder_between_root_and_agg(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  
  if (lop_info->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
    return true;
  } else if (lop_info->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    return false;
  }
  
  if (lop_info->children.empty()) { return false; }

  return reorder_between_root_and_agg(qid, lop_info->children[0]);
}


// TODO: use idx_t instead of int
// iterate top down , when order by is encountered, then 
void AdjustOutputIds(idx_t qid, idx_t opid, vector<int>& groups) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  
  bool has_reorder = reorder_between_root_and_agg(qid, opid);
  if (!has_reorder) return;
  // bw[oid] = iid;
  // iterate over bw, replace groups[i] = bw[ groups[i] ];
  if (lop_info->lineage1D.empty()) {
    string table = to_string(qid) + "_" + to_string(opid);
    vector<std::pair<Vector, int>>& chunked_lineage = LineageState::lineage_store[table];
    for (auto& lin_n :chunked_lineage) {
      int64_t* col = reinterpret_cast<int64_t*>(lin_n.first.GetData());
      idx_t count = lin_n.second;
      for (idx_t i = 0; i < count; i++) {
        lop_info->lineage1D.push_back(col[i]);
      }
    }
  }

  for (idx_t i=0; i < groups.size(); ++i) {
    if (groups[i] >= lop_info->lineage1D.size()) std::cout << "----->" << i << " " << lop_info->lineage1D[i] << " " << groups[i] << std::endl;
    groups[i] = lop_info->lineage1D[ groups[i] ];
  }
}

void compute_count_sum_sum2(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
   compute_count_sum_sum2(qid, child);
  }
  if (lop_info->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    for (auto &agg : lop_info->agg_info->aggs) {
      string func = agg.second->name;
      if (func == "count" || func == "avg" || func == "stddev") {
        agg.second->groups_count.assign(lop_info->n_output, 0);
        for (int i=0; i < lop_info->lineage1D.size(); ++i) {
          int oid = lop_info->lineage1D[i];
          agg.second->groups_count[oid]++;
        }
      }
      
      if (func == "sum" || func == "avg" || func == "stddev")  {
        string sum_agg_key = agg.second->sub_aggs[0];
        idx_t col_idx = lop_info->agg_info->sub_aggs[sum_agg_key]->payload_idx;
        auto typ = lop_info->agg_info->input_data_types_map[col_idx];
        agg.second->groups_sum.assign(lop_info->n_output, 0);
        if (typ == LogicalType::INTEGER) {
          int *in_arr = reinterpret_cast<int *>(lop_info->agg_info->input_data_map[col_idx]);
          for (int i=0; i < lop_info->lineage1D.size(); ++i) {
            int oid = lop_info->lineage1D[i];
             agg.second->groups_sum[oid] += in_arr[i];
          }
        } else {
          float *in_arr = reinterpret_cast<float *>(lop_info->agg_info->input_data_map[col_idx]);
          for (int i=0; i < lop_info->lineage1D.size(); ++i) {
            int oid = lop_info->lineage1D[i];
             agg.second->groups_sum[oid] += in_arr[i];
          }
        }
      }
      
      if (func == "stddev") {
        string sum_agg_key = agg.second->sub_aggs[0];
      
        idx_t col_idx = lop_info->agg_info->sub_aggs[sum_agg_key]->payload_idx;
        auto &typ = lop_info->agg_info->input_data_types_map[col_idx];
      
        agg.second->groups_sum_2.assign(lop_info->n_output, 0);
        if (typ == LogicalType::INTEGER) {
          int *in_arr = reinterpret_cast<int *>(lop_info->agg_info->input_data_map[col_idx]);
          for (int i=0; i < lop_info->lineage1D.size(); ++i) {
            int oid = lop_info->lineage1D[i];
            agg.second->groups_sum_2[oid] += (in_arr[i] * in_arr[i]);
          }
        } else {
          float *in_arr = reinterpret_cast<float *>(lop_info->agg_info->input_data_map[col_idx]);
          for (int i=0; i < lop_info->lineage1D.size(); ++i) {
            int oid = lop_info->lineage1D[i];
            agg.second->groups_sum_2[oid] += (in_arr[i] * in_arr[i]);
          }
        }
      }
    }
  }
}

pair<int, int> get_start_end(int row_count, int thread_id, int num_worker) {
	int batch_size = row_count / num_worker;
	if (row_count % num_worker > 0) batch_size++;
	int start = thread_id * batch_size;
	int end   = start + batch_size;
	if (end >= row_count)  end = row_count;
	return std::make_pair(start, end);
}

template<class T>
void allocate_agg_output(FadeNode* fnode, int t, int n_interventions,
    int n_output, string out_var) {
	fnode->alloc_vars[out_var][t] = aligned_alloc(64, sizeof(T) * n_output * n_interventions);
	if (fnode->alloc_vars[out_var][t] == nullptr) {
		fnode->alloc_vars[out_var][t] = malloc(sizeof(T) * n_output * n_interventions);
	}
	memset(fnode->alloc_vars[out_var][t], 0, sizeof(T) * n_output * n_interventions);
}

template<class T1, class T2>
T2* GetInputVals(LineageInfoNode *lop_info, idx_t col_idx) {
	idx_t chunk_count = lop_info->agg_info->cached_cols_sizes.size();
	idx_t row_count = lop_info->n_input;
	T2* input_values = new T2[row_count];
  
  if (chunk_count == 0) return input_values;
	auto typ = lop_info->agg_info->cached_cols[col_idx][0].GetType();
	idx_t offset = 0;
	for (idx_t i=0; i < chunk_count; ++i) {
		T1* col = reinterpret_cast<T1*>(lop_info->agg_info->cached_cols[col_idx][i].GetData());
		int count = lop_info->agg_info->cached_cols_sizes[i];
		Vector new_vec(LogicalType::FLOAT, count);
		if (typ.id() == LogicalTypeId::DECIMAL) {
			CastParameters parameters;
			uint8_t width = DecimalType::GetWidth(typ);
			uint8_t scale = DecimalType::GetScale(typ);
			switch (typ.InternalType()) {
			case PhysicalType::INT16: {
				VectorCastHelpers::TemplatedDecimalCast<int16_t, float, TryCastFromDecimal>(
				    lop_info->agg_info->cached_cols[col_idx][i], new_vec, count, parameters, width, scale);
				break;
			} case PhysicalType::INT32: {
				VectorCastHelpers::TemplatedDecimalCast<int32_t, float, TryCastFromDecimal>(
				    lop_info->agg_info->cached_cols[col_idx][i], new_vec, count, parameters, width, scale);
				break;
			} case PhysicalType::INT64: {
				VectorCastHelpers::TemplatedDecimalCast<int64_t, float, TryCastFromDecimal>(
				    lop_info->agg_info->cached_cols[col_idx][i], new_vec, count, parameters, width, scale);
				break;
			} case PhysicalType::INT128: {
				VectorCastHelpers::TemplatedDecimalCast<hugeint_t, float, TryCastFromDecimal>(
				    lop_info->agg_info->cached_cols[col_idx][i], new_vec, count, parameters, width, scale);
				break;
			} default: {
				throw InternalException("Unimplemented internal type for decimal");
			}
			}
			col = reinterpret_cast<T1*>(new_vec.GetData());
		}
    // TODO: handle null values
		for (idx_t i=0; i < count; ++i) {
			input_values[i+offset] = col[i];
		}
		offset +=  count;
	}

	return input_values;
}

void get_cached_vals(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
   get_cached_vals(qid, child);
  }
  if (lop_info->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    int n_input = lop_info->n_input;
    // TODO: have lop_info->agg_info->payload_index instead of iterating over functions to figure out which
    // column to read
    for (auto& out_var : lop_info->agg_info->payload_data) {
      int col_idx = out_var.first;
      if (lop_info->agg_info->cached_cols[col_idx].empty()) continue;
      if (LineageState::debug) std::cout << "GetCachedVals " << col_idx << " " << n_input << std::endl;
      auto in_typ = lop_info->agg_info->cached_cols[col_idx][0].GetType();
      if (in_typ == LogicalType::INTEGER) {
        lop_info->agg_info->input_data_types_map[col_idx] = LogicalType::INTEGER;
        lop_info->agg_info->input_data_map[col_idx] = GetInputVals<int, int>(lop_info.get(), col_idx);
      } else if (in_typ == LogicalType::BIGINT) {
          lop_info->agg_info->input_data_types_map[col_idx] = LogicalType::INTEGER;
          lop_info->agg_info->input_data_map[col_idx] = GetInputVals<int64_t, int>(lop_info.get(), col_idx);
        } else if (in_typ == LogicalType::FLOAT) {
          lop_info->agg_info->input_data_types_map[col_idx] = LogicalType::FLOAT;
          lop_info->agg_info->input_data_map[col_idx] = GetInputVals<float, float>(lop_info.get(), col_idx);
        } else if (in_typ == LogicalType::DOUBLE) {
          lop_info->agg_info->input_data_types_map[col_idx] = LogicalType::FLOAT;
          lop_info->agg_info->input_data_map[col_idx] = GetInputVals<double, float>(lop_info.get(), col_idx);
        } else {
          lop_info->agg_info->input_data_types_map[col_idx] = LogicalType::FLOAT;
          lop_info->agg_info->input_data_map[col_idx] = GetInputVals<float, float>(lop_info.get(), col_idx);
        }
    }
  }
}

void prepare_fade_plan(idx_t qid, idx_t opid, std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                      unordered_map<string, vector<string>>& spec_map) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
   prepare_fade_plan(qid, child, fade_data, spec_map);
  }

	unique_ptr<FadeSparseNode> sfnode = make_uniq<FadeSparseNode>(qid, opid);

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      if (spec_map.find(lop_info->table_name) != spec_map.end()) {
        sfnode->n_interventions = 1;
        for (const string  &col : spec_map[lop_info->table_name]) {
          string spec_key = lop_info->table_name + "." + col;
          sfnode->n_interventions *= FadeState::col_n_unique[spec_key];
          // todo: this should be local to the whatif instantiation
          FadeState::cached_spec_stack.push_back(spec_key);
        }
        sfnode->annotations.reset(new int[lop_info->n_input]); // TODO: if spec is single array, avoid this
        if (LineageState::debug)
          std::cout << "Table Name: " << lop_info->table_name <<  ", n_interventions: " << sfnode->n_interventions << std::endl;
      }
      break;
   } case LogicalOperatorType::LOGICAL_FILTER: {
      sfnode->n_interventions = fade_data[lop_info->children[0]]->n_interventions;
      sfnode->annotations.reset(new int[lop_info->n_output]);
      break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      idx_t lhs_n = fade_data[lop_info->children[0]]->n_interventions;
      idx_t rhs_n = fade_data[lop_info->children[1]]->n_interventions;
      sfnode->n_interventions = (lhs_n > 0) ? lhs_n : rhs_n;
      if (LineageState::debug) std::cout << "LHS_N. " << lhs_n << " RHS_N. " << rhs_n << std::endl;
      if (sfnode->n_interventions > 1) sfnode->annotations.reset(new int[lop_info->n_output]);
      break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      sfnode->n_interventions = fade_data[lop_info->children[0]]->n_interventions;
      int n_output = lop_info->n_output;
      // alloc per worker t. n_output X n_interventions
      for (auto &sub_agg : lop_info->agg_info->sub_aggs) {
        string out_key = sub_agg.first;
        auto &typ = sub_agg.second->return_type;
			  sfnode->alloc_vars[out_key].resize(sfnode->num_worker);
        for (int t=0; t < sfnode->num_worker; ++t) {
          if (typ == LogicalType::INTEGER)
            allocate_agg_output<int>(sfnode.get(), t, sfnode->n_interventions, n_output, out_key);
          else
            allocate_agg_output<float>(sfnode.get(), t, sfnode->n_interventions, n_output, out_key);
        }
      }
     break;
   } default: {}}
  
  if (LineageState::debug)
    std::cout << "prep fade exit:" << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << ", n_input:" <<
      lop_info->n_input << ", n_output:" << lop_info->n_output << ", n_interventions:" << sfnode->n_interventions << std::endl;
    
   fade_data[opid] = std::move(sfnode);
}

// table_name.col_name|...|table_name.col_name
unordered_map<string, vector<string>> parse_spec(vector<string>& cols_spec) {
	unordered_map<string, vector<string>> result;
  for (idx_t i = 0; i < cols_spec.size(); ++i) {
		string table, column;
    string& table_col = cols_spec[i];
    std::istringstream ss(table_col);
    if (LineageState::debug)
      std::cout << "parse: " << table_col << std::endl;
		if (std::getline(ss, table, '.') && std::getline(ss, column)) {
      // Convert column name to uppercase (optional)
      for (char& c : column) 	c = std::tolower(c);
      // Add the table name and column to the dictionary
      result[table].push_back(column);
		}
  }

	return result;
}

unique_ptr<MaterializedQueryResult> get_codes(ClientContext &context, string table, string col) {
  auto conn = make_uniq<Connection>(*context.db);
  std::ostringstream query;
  query << "SELECT DISTINCT v FROM (SELECT CAST( DENSE_RANK() OVER (ORDER BY "
    << col << ") AS INTEGER ) - 1 AS k, "
    << col << " as v FROM " << table << " order by rowid) as t ORDER BY k";
  if (LineageState::debug)  std::cout << query.str() << std::endl;
  auto result = conn->Query(query.str());
  if (!result || result->HasError()) {
      std::cerr << "Query failed: " << (result ? result->GetError() : "null result") << std::endl;
      return nullptr;
  }
  idx_t count = result->RowCount();
  if (LineageState::debug)
    std::cout << result->ToString() << std::endl;
  return std::move(result);
}

string generate_dict_query(const string &table, const vector<string> &columns) {
    std::ostringstream query;
    auto &map = FadeState::table_col_annotations[table];
    query << "SELECT \n";
    bool subquery = false;
    for (size_t i = 0; i < columns.size(); ++i) {
      if (map.find(columns[i]) != map.end()) continue;
      query << "CAST( DENSE_RANK() OVER (ORDER BY " << columns[i] << ") AS INTEGER ) - 1 AS "
              << columns[i] << "_int";
      if (i != columns.size() - 1) { query << ",\n"; } else { query << "\n"; }
      subquery = true;
    }
    query << "FROM " << table << " ORDER BY rowid;";
    if (!subquery) return "";
    return query.str();
}

string generate_n_unique_count(const string &table, const vector<string> &columns) {
    std::ostringstream query;
    auto &map = FadeState::table_col_annotations[table];
    query << "SELECT \n";
    bool subquery = false;
    for (size_t i = 0; i < columns.size(); ++i) {
      if (map.find(columns[i]) != map.end()) continue;
      query << "COUNT(DISTINCT  " << columns[i] << ") ";
      if (i != columns.size() - 1) { query << ",\n"; } else { query << "\n"; }
      subquery = true;
    }
    query << "FROM " << table << ";";
    if (!subquery) return "";
    return query.str();
}


void read_annotations(ClientContext &context, unordered_map<string, vector<string>>& spec) {
  auto conn = make_uniq<Connection>(*context.db);
  for (const auto& e : spec) {
    string query = generate_dict_query(e.first, e.second);
    if (LineageState::debug) std::cout << query << std::endl;
    if (query.empty()) continue;
    auto result = conn->Query(query);
    if (!result || result->HasError()) {
        std::cerr << "Query failed: " << (result ? result->GetError() : "null result") << std::endl;
        return;
    }
    idx_t count = result->RowCount();
    FadeState::table_count[e.first] = count;
    idx_t col_count = result->ColumnCount();
    vector<vector<int32_t>> result_vector(col_count);
    auto &collection = result->Collection();
    for (auto &chunk : collection.Chunks()) {
      for (idx_t col=0; col < col_count; ++col) {
        Vector &vec = chunk.data[col];  // first column vector
        auto count = chunk.size();
        // Get raw pointer to int32_t data
        auto ptr = FlatVector::GetData<int32_t>(vec);
        // Copy data from vector into result_vector
        result_vector[col].insert(result_vector[col].end(), ptr, ptr + count);
      }
    }
    query =  generate_n_unique_count(e.first, e.second);
    if (LineageState::debug)
      std::cout << "query: " << query << std::endl;
    result = conn->Query(query);
    if (!result || result->HasError()) {
        std::cerr << "Query failed: " << (result ? result->GetError() : "null result") << std::endl;
        return;
    }
    // std::cout << result->ToString() << std::endl;
    for (idx_t col=0; col < col_count; ++col) {
      FadeState::col_n_unique[e.first + "." + e.second[col]] = result->GetValue(col, 0).GetValue<int>();
    }

    auto &map = FadeState::table_col_annotations[e.first];
    for (size_t i = 0; i < e.second.size(); ++i) {
      if (map.find(e.second[i]) != map.end()) continue;
      map[e.second[i]] = std::move(result_vector[i]);
      if (LineageState::debug)
        std::cout << e.second[i] << " " << i << " " << map[e.second[i]].size() << std::endl;
    }
  }

}

} // namespace duckdb
