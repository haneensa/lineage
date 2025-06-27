#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

struct LineageReadBindData : public TableFunctionData {
  idx_t cardinality;
  idx_t chunk_count;
  string table_name;
  idx_t query_id;
  int operator_id;

  void Initialize() {
    cardinality = 0;
    chunk_count = 0;
    operator_id = -1;
    query_id = -1;
  }
};

struct LineageReadLocalState : public LocalTableFunctionState {
};

struct LineageReadGlobalState : public GlobalTableFunctionState {
};

class LineageScanFunction {
  public:
    static void LineageScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> LineageScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> LineageScanInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState>
    LineageScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p);
    static unique_ptr<TableRef> ReadLineageReplacement(ClientContext &context, ReplacementScanInput &input,
        optional_ptr<ReplacementScanData> data);
    static TableFunctionSet GetFunctionSet();
    static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data);
    static unique_ptr<BaseStatistics> ScanStats(ClientContext &context, const FunctionData *bind_data_p,
        column_t column_index);
};

} // namespace duckdb
