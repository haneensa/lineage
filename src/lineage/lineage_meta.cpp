#include "lineage/lineage_meta.hpp"

#include "lineage/lineage_init.hpp"
#include "lineage/lineage_plan.hpp"

#include <iostream>

namespace duckdb {

// Table function that exposes the lineage plan for each query
// as a JSON-encoded logical tree.
//
// Schema:
//   query_id INTEGER
//   plan     VARCHAR (JSON)
void LineageMetaFunction::Implementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->CastNoConst<BindData>();
  output.SetCardinality(0);

  idx_t count = LineageState::qid_plans_roots.size();
  idx_t start = bind_data.offset;
  idx_t limit = std::min<idx_t>(STANDARD_VECTOR_SIZE, count - start);

  if (start >= count) return;

  output.data[0].Sequence(start, 1, limit);
  for (idx_t i=start; i < limit+start; i++) {
    idx_t root = LineageState::qid_plans_roots[i];
    string plan_json = serialize_to_json(i, root);
    output.data[1].SetValue(i, Value(plan_json));
  }
  output.SetCardinality(limit);
  bind_data.offset += limit;
}

unique_ptr<FunctionData> LineageMetaFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<BindData>();

  names.emplace_back("query_id");
  return_types.emplace_back(LogicalType::INTEGER);

  names.emplace_back("plan");
  return_types.emplace_back(LogicalType::VARCHAR);

  return std::move(result);
}

} // namespace duckdb
