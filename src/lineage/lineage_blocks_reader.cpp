#include "lineage/lineage_blocks_reader.hpp"

#include "lineage/lineage_init.hpp"
#include "lineage/lineage_plan.hpp"

#include <iostream>

namespace duckdb {

// Table function that read lineage blocks
//
// Schema:
//   output_id INTEGER
//   input_id INTEGER
void BlockReaderFunction::Implementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->CastNoConst<BlockReaderBindData>();
  idx_t qid = bind_data.qid;

  JoinAggBlocks& join_agg_block =  LineageState::lineage_blocks[qid].back();
  idx_t count = join_agg_block.n;
  idx_t start = bind_data.offset;
  idx_t limit = std::min<idx_t>(STANDARD_VECTOR_SIZE, count - start);
  if (start >= count) return;

  // forward lineage
  idx_t cols = 0;
  if (join_agg_block.forward_lineage.empty()) {
    output.data[cols++].Sequence(start, 1, limit);
  } else {
    data_ptr_t ptr = (data_ptr_t)(join_agg_block.forward_lineage.data() + start);
    Vector in_index(LogicalType::BIGINT, ptr);
    output.data[cols++].Reference(in_index);
  }
  for (auto& elm : join_agg_block.srcs_lineage) {
    data_ptr_t ptr = (data_ptr_t)(elm.second.data() + start);
    Vector in_index(LogicalType::BIGINT, ptr);
    output.data[cols++].Reference(in_index);
  }
  
  output.SetCardinality(limit);
  bind_data.offset += limit;
}

unique_ptr<FunctionData> BlockReaderFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

    // Validate argument count
  if (input.inputs.size() != 1) {
    throw BinderException("read_block expects exactly one argument");
  }

  // Extract integer argument
  auto qid = input.inputs[0].GetValue<int32_t>();

  auto result = make_uniq<BlockReaderBindData>();
  result->qid = qid;

  names.emplace_back("output_id");
  return_types.emplace_back(LogicalType::BIGINT);

  JoinAggBlocks& join_agg_block =  LineageState::lineage_blocks[qid].back();
  for (auto& elm : join_agg_block.srcs_lineage) {
    idx_t opid = elm.first;
    auto &lop_info = LineageState::qid_plans[qid][opid];
    names.emplace_back("opid_" + to_string(opid) + "_" + lop_info->table_name);
    return_types.emplace_back(LogicalType::BIGINT);
  }

  return std::move(result);
}

} // namespace duckdb
