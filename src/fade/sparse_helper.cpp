#include "lineage/lineage_init.hpp"
#include "fade/fade.hpp"

#include <iostream>
#include <string>

namespace duckdb {

// table_name.col_name|...|table_name.col_name
unordered_map<string, vector<string>>  parse_spec(vector<Value>& cols_spec) {
	unordered_map<string, vector<string>> result;
  for (idx_t i = 0; i < cols_spec.size(); ++i) {
		string table, column;
    string table_col = cols_spec[i].ToString();
    std::istringstream ss(table_col);
    // std::cout << "parse: " << table_col << std::endl;
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
  std::cout << query.str() << std::endl;
  auto result = conn->Query(query.str());
  if (!result || result->HasError()) {
      std::cerr << "Query failed: " << (result ? result->GetError() : "null result") << std::endl;
      return nullptr;
  }
  idx_t count = result->RowCount();
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
  std::cout << "read_annotations" << std::endl;
  auto conn = make_uniq<Connection>(*context.db);
  for (const auto& e : spec) {
    string query = generate_dict_query(e.first, e.second);
    std::cout << query << std::endl;
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
      std::cout << e.second[i] << " " << i << " " << map[e.second[i]].size() << std::endl;
    }
  }
}

template<class IN_T, class OUT_T>
void allocate_total_agg_output(string fname, string qid_opid, idx_t col_idx,
    int n_output, string out_var) {
  FadeState::alloc_typ_vars[qid_opid][out_var].second[0] = malloc(sizeof(OUT_T) * n_output * 1 /*n_interventions*/);
	memset(FadeState::alloc_typ_vars[qid_opid][out_var].second[0], 0, sizeof(OUT_T) * n_output * 1 /*n_interventions*/);
  vector<vector<idx_t>>& lineage = LineageState::lineage_global_store[qid_opid];
  OUT_T* out = (OUT_T*)FadeState::alloc_typ_vars[qid_opid][out_var].second[0];
  if (fname == "count") {
    for (idx_t oid=0; oid < lineage.size(); ++oid) {
      out[oid] = lineage[oid].size();
    }
  } else if (fname == "sum") {
    IN_T *in_arr = reinterpret_cast<IN_T *>(FadeState::input_data_map[qid_opid][col_idx].second);
    for (idx_t oid=0; oid < lineage.size(); ++oid) {
      for (idx_t i=0; i < lineage[oid].size(); ++i) {
        idx_t iid = lineage[oid][i];
        out[oid] +=  in_arr[iid];
      }
    }
  } else if (fname == "sum_2") {
    IN_T *in_arr = reinterpret_cast<IN_T *>(FadeState::input_data_map[qid_opid][col_idx].second);
    for (idx_t oid=0; oid < lineage.size(); ++oid) {
      for (idx_t i=0; i < lineage[oid].size(); ++i) {
        idx_t iid = lineage[oid][i];
        out[oid] +=  (in_arr[iid] * in_arr[iid]);
      }
    }
  }

}

void RecomputeAggs(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
    RecomputeAggs(qid, child);
  }

  string qid_opid = to_string(qid) + "_" + to_string(opid);

  switch (lop_info->type) {
   case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      int n_output = lop_info->n_output;
      for (auto &sub_agg : FadeState::sub_aggs[qid_opid]) {
        auto &typ = sub_agg.second->return_type;
        idx_t col_idx = sub_agg.second->payload_idx;
        string& func = sub_agg.second->name;
        string out_key = "total_" + sub_agg.first;
        if (FadeState::debug)  std::cout << "aggs => " << out_key << std::endl;
        FadeState::alloc_typ_vars[qid_opid][out_key].second.assign(1 /* num_worker */, 0);
        FadeState::alloc_typ_vars[qid_opid][out_key].first = LogicalType::FLOAT;
        if (func == "count") {
          FadeState::alloc_typ_vars[qid_opid][out_key].first = LogicalType::INTEGER;
          allocate_total_agg_output<int, int>(func, qid_opid, col_idx, n_output, out_key);
        } else if (FadeState::input_data_map[qid_opid][col_idx].first == LogicalType::INTEGER) {
          allocate_total_agg_output<int, float>(func, qid_opid, col_idx, n_output, out_key);
        } else {
          allocate_total_agg_output<float, float>(func, qid_opid, col_idx, n_output, out_key);
        }
      }
   } default: {}}
}


// separate selection vector allocation for sparse and dense
idx_t PrepareSparseFade(idx_t qid, idx_t opid, idx_t agg_idx,
                      unordered_map<idx_t, FadeNode>& fade_data,
                      unordered_map<string, vector<string>>& spec_map) {
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

}
