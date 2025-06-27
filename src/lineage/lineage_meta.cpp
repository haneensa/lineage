#include "lineage/lineage_meta.hpp"

#include "lineage/lineage_init.hpp"

namespace duckdb {

// currently only returns latest query_id
void LineageMetaFunction::LineageMetaImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->CastNoConst<LineageMetaBindData>();

  if (bind_data.chunk_count > 0) return;

  output.data[0].Sequence(0, 1, LineageState::query_id-1);

  output.SetCardinality(1);
  bind_data.chunk_count++;
}

unique_ptr<FunctionData> LineageMetaFunction::LineageMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<LineageMetaBindData>();

  names.emplace_back("query_id");
  return_types.emplace_back(LogicalType::INTEGER);
  
  return std::move(result);
}

} // namespace duckdb
