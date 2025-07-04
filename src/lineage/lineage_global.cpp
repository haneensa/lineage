#include "lineage/lineage_global.hpp"

#include "lineage/lineage_init.hpp"
#include <iostream>
#include <string>

namespace duckdb {

void LineageGFunction::LineageGImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->CastNoConst<LineageGBindData>();
  auto &state = data_p.global_state->Cast<LineageGGlobalState>();

  if (state.finished) {
        return;
  }

  auto chunk = state.result->Fetch();
  if (!chunk || chunk->size() == 0) {
    state.finished = true;
    return;
  }
  output.Reference(*chunk);
}

unique_ptr<FunctionData> LineageGFunction::LineageGBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<LineageGBindData>();

  names.emplace_back("source_table");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("sink_table");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("source_opid");
  return_types.emplace_back(LogicalType::INTEGER);
  names.emplace_back("sink_opid");
  return_types.emplace_back(LogicalType::INTEGER);
  names.emplace_back("out_rowid");
  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("in_rowid");
  return_types.emplace_back(LogicalType::ROW_TYPE);
  
  return std::move(result);
}

void get_per_op_lineage_query(idx_t qid, idx_t root, vector<string>& queries) {
  auto &lop = LineageState::qid_plans[qid][root];
  
  std::ostringstream oss;
  if (lop->has_lineage) {
    int opid = root;
    string name = EnumUtil::ToChars<LogicalOperatorType>(lop->type);
    bool needs_unnest = lop->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY
                        || lop->type == LogicalOperatorType::LOGICAL_DELIM_GET;
    string sink_table = lop->table_name.empty() ? name : lop->table_name;
    int sink_opid = lop->sink_id;
    int src_opid = lop->source_id[0];
    string src_table = LineageState::qid_plans[qid][src_opid]->table_name;
    string src_name = EnumUtil::ToChars<LogicalOperatorType>(LineageState::qid_plans[qid][src_opid]->type);
    src_table = src_table.empty() ? src_name : src_table;
    if (needs_unnest) {
      oss << "SELECT '" << src_table << "', '" << sink_table << "', "<< src_opid << ", ";
      oss << sink_opid << ", out_rowid, in_elem FROM (";
      oss << "SELECT out_rowid, UNNEST(in_rowid) AS in_elem ";
      oss << "FROM lineage_scan(" << qid << ", " << opid;
      oss << ", 0) AS ls(out_rowid BIGINT, in_rowid LIST(BIGINT)))";
    } else {
      oss << "SELECT '" << src_table << "', '" << sink_table << "', "<< src_opid << ", ";
      oss << sink_opid << ", out_rowid, in_rowid ";
      oss << " FROM lineage_scan(" << qid << ", " << opid;
      oss << ", 0) AS ls(out_rowid BIGINT, in_rowid BIGINT)";
    }

    queries.push_back(oss.str());
    if (lop->children.size() > 1) {
      std::ostringstream oss2;
      src_opid = lop->source_id[1];
      src_table = LineageState::qid_plans[qid][src_opid]->table_name;
      src_name = EnumUtil::ToChars<LogicalOperatorType>(LineageState::qid_plans[qid][src_opid]->type);
      src_table = src_table.empty() ? src_name : src_table;
      oss2 << "SELECT '" << src_table << "', '" << sink_table << "', "<< src_opid << ", ";
      oss2 << sink_opid << ", out_rowid, in_rowid ";
      oss2 << " FROM lineage_scan(" << qid << ", " << opid;
      oss2 << ", 1) AS ls(out_rowid BIGINT, in_rowid BIGINT)";
      queries.push_back(oss2.str());
    }
  }

  for (size_t i = 0; i < lop->children.size(); i++) {
    get_per_op_lineage_query(qid, lop->children[i], queries);
  }
}

unique_ptr<GlobalTableFunctionState> LineageGFunction::LineageGInit(ClientContext &context,
                                                 TableFunctionInitInput &input) {
  // use the latest lineage
  vector<string> queries;
  idx_t last_qid = LineageState::qid_plans_roots.size()-1;
  idx_t root = LineageState::qid_plans_roots[last_qid];
  get_per_op_lineage_query(last_qid, root, queries);
  string query;
  std::ostringstream oss;
  if (queries.empty()) {
    query = "SELECT 'none' as source_table, 'none' as sink_table, CAST(-1 AS INT) as source_opid, CAST(-1 AS INT) as sink_opid, CAST(-1 AS BIGINT) as out_rowid, CAST(-1 AS BIGINT) as in_rowid where 1<>2";
  } else {
  }

  for (auto& q : queries) {
    if (oss.str().empty()) oss << q;
    else  oss << "\nUNION ALL " << q;
  }

  query = oss.str();
  if (LineageState::debug) std::cout << query << std::endl;
  auto conn = make_uniq<Connection>(*context.db);

  auto result = conn->Query(query);

  if (!result || result->HasError()) {
    throw Exception(ExceptionType::EXECUTOR, "Failed to execute internal query: " + result->GetError());
  }

  return make_uniq<LineageGGlobalState>(std::move(result));
}

} // namespace duckdb
