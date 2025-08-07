#include "lineage/lineage_query.hpp"

#include "lineage/lineage_init.hpp"
#include <iostream>
#include <string>

namespace duckdb {

void LQFunction::LQImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->CastNoConst<LQBindData>();
  auto &gstate = data_p.global_state->Cast<LQGlobalState>();
  if (gstate.source_iter == gstate.out_per_src.end()) return; 

  auto& inner = gstate.source_iter->second[ gstate.outer_cur ]->iids;
  if (gstate.inner_offset >= inner.size()) {
    gstate.outer_cur++;
    gstate.inner_offset = 0;
    if (gstate.outer_cur >= gstate.source_iter->second.size()) {
      gstate.source_iter++;
      gstate.outer_cur = 0;
    }
    if (gstate.source_iter == gstate.out_per_src.end()) return; 
    inner = gstate.source_iter->second[ gstate.outer_cur ]->iids;
  }
  idx_t remaining = inner.size() - gstate.inner_offset;
  idx_t count = remaining  > STANDARD_VECTOR_SIZE ? STANDARD_VECTOR_SIZE : remaining;
  
  Vector source_vec(Value::INTEGER(gstate.source_iter->first));
  output.data[0].Reference(source_vec);
  
  
  Vector join_id_vec(Value::BIGINT(gstate.source_iter->second[gstate.outer_cur]->join_id));
  output.data[1].Reference(join_id_vec);
  
  // if gsate.sc.nested: iid -> oid
  // join, iid, ith+length_so_far
  // how to combine SC for non-leaf operators?
  // else
  // break pipelines around aggs; all joins below an agg we can join
  // them based on position
  output.data[2].Sequence(gstate.inner_offset, 1, count); // in_index
  
  Vector oid_vec(Value::BIGINT(0));
  output.data[3].Reference(oid_vec);
                                            
  data_ptr_t ptr = (data_ptr_t)(inner.data() + gstate.inner_offset);
  Vector in_index(LogicalType::BIGINT, ptr);
  output.data[4].Reference(in_index); // in_index

  output.SetCardinality(count);
  gstate.inner_offset += count;
}

unique_ptr<FunctionData> LQFunction::LQBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<LQBindData>();

  // names.emplace_back("source_table");
  // return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("source_opid");
  return_types.emplace_back(LogicalType::INTEGER);
  
  names.emplace_back("join_id"); // PK -> join id
  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("join_oid"); // PK -> join id
  return_types.emplace_back(LogicalType::ROW_TYPE);
  
  names.emplace_back("out_rowid");
  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("in_rowid");
  return_types.emplace_back(LogicalType::ROW_TYPE);
  
  return std::move(result);
}

unique_ptr<GlobalTableFunctionState> LQFunction::LQInit(ClientContext &context,
                                                 TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->CastNoConst<LQBindData>();
  // use the latest lineage
  vector<string> queries;
  idx_t last_qid = LineageState::qid_plans_roots.size()-1;
  idx_t root = LineageState::qid_plans_roots[last_qid];

  // partition_id -> {iids}
  vector<idx_t> oids_per_partition;
  unordered_map<idx_t, vector<shared_ptr<SourceContext>>> out_per_src;
  oids_per_partition.emplace_back(bind_data.oid);
  LQ(last_qid, root, oids_per_partition, out_per_src);
  std::cout << "|sources| : " << out_per_src.size() << std::endl;

  return make_uniq<LQGlobalState>(std::move(out_per_src));
}

} // namespace duckdb
