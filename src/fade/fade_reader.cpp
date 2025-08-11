#include "fade/fade_reader.hpp"

#include "fade/fade.hpp"

#include <iostream>

namespace duckdb {

struct FadeReaderBindData : public TableFunctionData {
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
  return;
  /*
  if (!data_p.local_state) return;
  auto &data = data_p.local_state->Cast<FadeReaderLocalState>();
  auto &gstate = data_p.global_state->Cast<FadeReaderGlobalState>();
  auto &bind_data = data_p.bind_data->CastNoConst<FadeReaderBindData>();

  auto fnode_iter = FadeState::fade_results.find(bind_node->qid);
  if (fnode_iter == FadeState::fade_result.end()) return;
  auto &fnode = fnode_iter->second;
  idx_t out_var = bind_data.out_var;  // 1) get out_var idx
  auto &lop_info = LineageState::qid_plans[bind_node->qid][fnode->opid];
  string fname = bind_data.fname;
  string out_key = fname + "_" + to_string(out_var);

  // std::cout << "fade reader implementation " << fname << " " << out_var << " " << out_key << std::endl;
	if (data.offset >= bind_data.n_interventions)	return; // finished returning values

  // start returning values
  // either fill up the chunk or return all the remaining columns
  idx_t col = 1;
	idx_t count =  = bind_data.n_interventions - data.offset;
  if (count >= STANDARD_VECTOR_SIZE) {
    count = STANDARD_VECTOR_SIZE;
  }
  
  for (int i=0; i < bind_data.n_groups; ++i) {
    int index = i * bind_data.n_interventions + data.offset;
    idx_t g = i;
    if (!fnode->oids.empty()) g = fnode->oids[i];
    output.data[col].Initialize(true, count);
    IReader<fname>(out_var, output, g, col, count);
    col++;
  }

  output.data[0].Sequence(data.offset, 1, count);

  data.offset += count;
	output.SetCardinality(count);*/
}

unique_ptr<FunctionData> FadeReaderFunction::FadeReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<FadeReaderBindData>();
  /*
  result->out_var = 0; // 1) get out_var idx using agg_id
  result->qid = 0;

  auto fnode_iter = FadeState::fade_results.find(result->qid);
  if (fnode_iter == FadeState::fade_result.end()) return std::move(result);
  
  auto &fnode = fnode_iter->second;
  
  names.emplace_back("pid");
  return_types.emplace_back(LogicalType::INTEGER);

  auto &agg_context = FadeState::aggs[result->out_var];
  
  LogicalType &ret_type = agg_context->return_type;
  result->fname = agg_context->name;
  result->n_interventions = fnode->n_interventions;
  result->n_groups = 0;

  if (!fnode->oids.empty()) {
    for (int i=0; i < fnode->oids.size(); ++i) {
      idx_t g = fnode->oids[i];
      if (g >= fnode->n_output) continue;
      result->n_groups++;
      names.emplace_back("g" + to_string(i));
      return_types.emplace_back(ret_type);
    }
  } else {
    result->n_groups = fnode->n_output;
    for (int i=0; i < fnode->n_output; ++i) {
      names.emplace_back("g" + to_string(i));
      return_types.emplace_back(ret_type);
    }
  }*/

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
