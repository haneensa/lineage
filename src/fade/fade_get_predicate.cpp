#include "fade/fade_get_predicate.hpp"

#include "fade_extension.hpp"
#include "lineage/lineage_init.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

namespace duckdb {

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
  std::cout << "specs_stack: " << std::endl;
  for (idx_t i=0; i < specs_stack.size(); ++i) {
    std::cout << i << specs_stack[i] << std::endl;
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

void GetPredicatesFunction::GetPredicatesImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  if (!data_p.local_state) return;
  auto &data = data_p.local_state->Cast<GetPredicatesLocalState>();
  auto &gstate = data_p.global_state->Cast<GetPredicatesGlobalState>();
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

unique_ptr<GlobalTableFunctionState> GetPredicatesFunction::GetPredicatesInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    return make_uniq<GetPredicatesGlobalState>();
}

unique_ptr<TableRef> GetPredicatesFunction::GetPredicatesReplacement(ClientContext &context, ReplacementScanInput &input,
    optional_ptr<ReplacementScanData> data) {
  auto table_name = ReplacementScan::GetFullPath(input);

  if (!ReplacementScan::CanReplace(table_name, {"get_predicate"})) {
    return nullptr;
  }
  
  auto table_function = make_uniq<TableFunctionRef>();
  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
  table_function->function = make_uniq<FunctionExpression>("get_predicate", std::move(children));

  if (!FileSystem::HasGlob(table_name)) {
    auto &fs = FileSystem::GetFileSystem(context);
    table_function->alias = fs.ExtractBaseName(table_name);
  }

  return std::move(table_function);
}

unique_ptr<NodeStatistics> GetPredicatesFunction::Cardinality(ClientContext &context, const FunctionData *bind_data) {
  auto &data = bind_data->CastNoConst<GetPredicatesBindData>();
  return make_uniq<NodeStatistics>(10);
}
unique_ptr<BaseStatistics> GetPredicatesFunction::ScanStats(ClientContext &context,
    const FunctionData *bind_data_p, column_t column_index) {
  auto &bind_data = bind_data_p->CastNoConst<GetPredicatesBindData>();
  auto stats = NumericStats::CreateUnknown(LogicalType::ROW_TYPE);
  NumericStats::SetMin(stats, Value::BIGINT(0));
  NumericStats::SetMax(stats, Value::BIGINT(10));
  stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES); // depends on the type of operator
  return stats.ToUnique();
}

TableFunctionSet GetPredicatesFunction::GetFunctionSet() {
  // predicate_id:int
  TableFunction table_function("get_predicate", {LogicalType::INTEGER}, GetPredicatesImplementation,
      GetPredicatesBind, GetPredicatesInitGlobal, GetPredicatesInitLocal);

  table_function.statistics = ScanStats;
  table_function.cardinality = Cardinality;
  table_function.projection_pushdown = true;
  table_function.filter_pushdown = false;
  table_function.filter_prune = false;
  return MultiFileReader::CreateFunctionSet(table_function);
}
  
} // namespace duckdb
