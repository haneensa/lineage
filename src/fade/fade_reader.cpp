#include "fade/fade_reader.hpp"

#include "fade/fade.hpp"

#include <iostream>

namespace duckdb {

struct FadeReaderBindData : public TableFunctionData {
  string qid_opid;
  idx_t qid;
  idx_t out_var;
  string fname;
  idx_t n_interventions;
  idx_t n_groups;
};

struct FadeReaderLocalState : public LocalTableFunctionState {
  idx_t offset = 0;
};

struct FadeReaderGlobalState : public GlobalTableFunctionState {
};

void FadeReaderFunction::FadeReaderImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &data = data_p.local_state->Cast<FadeReaderLocalState>();
  auto &gstate = data_p.global_state->Cast<FadeReaderGlobalState>();
  auto &bdata = data_p.bind_data->CastNoConst<FadeReaderBindData>();

  auto fnode_iter = FadeState::fade_results.find(bdata.qid);
  if (fnode_iter == FadeState::fade_results.end()) return;
  auto &fnode = fnode_iter->second;
	if (data.offset >= bdata.n_interventions)	return; // finished returning values

  // start returning values
  // either fill up the chunk or return all the remaining columns
	idx_t count = bdata.n_interventions - data.offset;
  if (count >= STANDARD_VECTOR_SIZE) {
    count = STANDARD_VECTOR_SIZE;
  }
  
  idx_t col = 1;
  for (int i=0; i < bdata.n_groups; ++i) {
    int index = i * bdata.n_interventions + data.offset;
    idx_t row = i;
    if (!fnode.oids.empty()) row = fnode.oids[i].GetValue<idx_t>();
    output.data[col].Initialize(true, count);
    IReader(bdata.fname, bdata.qid_opid, fnode, bdata.out_var,  output, index, row, col, count);
    col++;
  }

  output.data[0].Sequence(data.offset, 1, count);
	output.SetCardinality(count);
  data.offset += count;
}

unique_ptr<FunctionData> FadeReaderFunction::FadeReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<FadeReaderBindData>();
  result->qid = input.inputs[0].GetValue<int>();
  idx_t agg_id = input.inputs[1].GetValue<int>(); // get out_var idx using agg_id
  result->out_var = agg_id;
  
  names.emplace_back("pid");
  return_types.emplace_back(LogicalType::INTEGER);
  
  auto fnode_iter = FadeState::fade_results.find(result->qid);
  if (fnode_iter == FadeState::fade_results.end()) return std::move(result);
  
  auto &fnode = fnode_iter->second;
  result->qid_opid = to_string(result->qid) + "_" + to_string(fnode.opid);

  
  if (FadeState::debug)
  std::cout << "found qid: " << result->qid_opid << " " << agg_id << " " << result->out_var << std::endl;
  
  auto acontext_iter = FadeState::aggs[result->qid_opid].find(result->out_var);
  if (acontext_iter == FadeState::aggs[result->qid_opid].end()) return std::move(result);
  auto &agg_context = acontext_iter->second;

  LogicalType &ret_type = agg_context->return_type;
  result->fname = agg_context->name;
  result->n_interventions = fnode.n_interventions;
  result->n_groups = 0;
  
  if (FadeState::debug)
  std::cout << "found out_var: " << result->out_var << " " << fnode.oids.size() << std::endl;

  result->n_groups = fnode.oids.empty() ? fnode.n_output : fnode.oids.size();
  for (int i=0; i < result->n_groups; ++i) {
    names.emplace_back("g" + to_string(i));
    return_types.emplace_back(ret_type);
  }

  return std::move(result);
}

unique_ptr<LocalTableFunctionState>
FadeReaderFunction::FadeReaderInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
    GlobalTableFunctionState *gstate_p) {
  return make_uniq<FadeReaderLocalState>();
}

unique_ptr<GlobalTableFunctionState> FadeReaderFunction::FadeReaderInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    return make_uniq<FadeReaderGlobalState>();
}

  
} // namespace duckdb
