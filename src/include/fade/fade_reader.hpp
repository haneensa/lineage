#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

struct FadeReaderBindData : public TableFunctionData {
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

class FadeReaderFunction {
  public:
    static void FadeReaderImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> FadeReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> FadeReaderInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState>
    FadeReaderInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p);
    static unique_ptr<TableRef> FadeReaderReplacement(ClientContext &context, ReplacementScanInput &input,
        optional_ptr<ReplacementScanData> data);
    static TableFunctionSet GetFunctionSet();
    static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data);
    static unique_ptr<BaseStatistics> ScanStats(ClientContext &context, const FunctionData *bind_data_p,
        column_t column_index);
};

} // namespace duckdb
