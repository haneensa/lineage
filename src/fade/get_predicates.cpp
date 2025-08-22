#include "fade/get_predicates.hpp"

#include "fade/fade.hpp"
#include "lineage/lineage_init.hpp"

#include <iostream>

namespace duckdb {

struct GetPredicatesBindData : public TableFunctionData {
  string res;
};

struct GetPredicatesLocalState : public LocalTableFunctionState {
  idx_t offset = 0;
};


/*
// the order of spec gives the order of how the annotations were combined
// [0, 1, 1, 0] * 4 + [0, 1, 2, 3]
// (0, 0), (1, 0), (2, 0), (3, 0), (0, 1), (1, 1), (2, 1), (3, 1)
// [0, 5, 6, 3]
// 5 % 4 = 1; (5 - 1) / 4 = 1
// 6 % 4 = 2; (6 - 2) / 4 = 1
// 3 % 3; (3 - 3) / 4 = 1 = 0
// (t.c1.n1),(t.c2.n2),
// codes: (lineitem.l_linestatus -> (0, 'O')), (lineitem.l_tax -> (0, '0.01'))
// specs_stack = [lineitem.l_linestatus, lineitem.l_tax]
// 7
// 0, 1, 2, 3, 4, 5, 6, 7
// 0 % 4, 1 % 4, 2 % 4, 3 % 4, 4 % 4, 5 % 4, 6 % 4, 7 % 4; shift = 0, size=1
// 0, 1, 2, 3, 0, 1, 2, 3; shift = 4, size=4
// 0, 1, 2, 3, 4, 5, 6, 7
// 0, 0, 0, 0, 4-0, 5-1, 6-2, 7-3
//
*/

// specs_stack = [lineitem.l_linestatus, lineitem.l_tax]
string get_predicate(int annotation) {
  auto &specs_stack = FadeState::cached_spec_stack;
  auto &codes_per_spec = FadeState::codes;
	int top = specs_stack.size() - 1;
  if (FadeState::debug) {
    std::cout << "specs_stack: " << std::endl;
    for (idx_t i=0; i < specs_stack.size(); ++i) {
      std::cout << i << specs_stack[i] << std::endl;
    }
  }
  

  string delim = "";
  int prev_shift = 0;
  string predicate = "";
	while (top > 0) {
	  int shift = codes_per_spec[specs_stack[top]]->RowCount(); // count
		int cur = annotation % shift;
    // get value for row at cur the second column as string
		predicate +=  delim + specs_stack[top] + "="
      + codes_per_spec[specs_stack[top]]->GetValue(0,cur).ToString();
    annotation = (annotation - cur) / shift;
		top--;
    delim = " AND ";
	}

  predicate +=   delim + specs_stack[top] + "="
    + codes_per_spec[specs_stack[top]]->GetValue(0,annotation).ToString();
	return predicate;
}

void GetPredicatesFunction::GetPredicatesImpl(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &data = data_p.local_state->Cast<GetPredicatesLocalState>();
  auto &bind_data = data_p.bind_data->CastNoConst<GetPredicatesBindData>();

  if (data.offset > 0) return;
  output.SetValue(0, 0, Value(bind_data.res));

  data.offset += 1;
	output.SetCardinality(1);
}

unique_ptr<FunctionData> GetPredicatesFunction::GetPredicatesBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<GetPredicatesBindData>();
  int annotation = input.inputs[0].GetValue<int>();
  
  names.emplace_back("predicate_str");
  return_types.emplace_back(LogicalType::VARCHAR);
  
  for (auto &spec : FadeState::cached_spec_map) {
    auto& table = spec.first;
    vector<string>& cols = spec.second;
    for (auto &col : cols) {
      string key = table + "." + col;
      if (FadeState::codes.find(key) == FadeState::codes.end()) {
        FadeState::codes[key] = get_codes(context, table, col);
      }
    }
  }
  
  result->res = get_predicate(annotation);

  return std::move(result);
}

unique_ptr<LocalTableFunctionState>
GetPredicatesFunction::GetPredicatesInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
    GlobalTableFunctionState *gstate_p) {
  return make_uniq<GetPredicatesLocalState>();
}

} // namespace duckdb
