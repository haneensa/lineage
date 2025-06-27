#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

struct GetPredicatesBindData : public TableFunctionData {
  string res;
};

struct GetPredicatesLocalState : public LocalTableFunctionState {
  idx_t offset = 0;
};

struct GetPredicatesGlobalState : public GlobalTableFunctionState {
};

class GetPredicatesFunction {
  public:
    static void GetPredicatesImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> GetPredicatesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> GetPredicatesInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState>
    GetPredicatesInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p);
    static unique_ptr<TableRef> GetPredicatesReplacement(ClientContext &context, ReplacementScanInput &input,
        optional_ptr<ReplacementScanData> data);
    static TableFunctionSet GetFunctionSet();
    static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data);
    static unique_ptr<BaseStatistics> ScanStats(ClientContext &context, const FunctionData *bind_data_p,
        column_t column_index);
};

} // namespace duckdb
