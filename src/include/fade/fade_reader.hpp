#pragma once

#include "fade/fade.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {


// generic reader that fills chunk with agg results
void IReader(string func, string qid_opid, FadeResult& fnode, idx_t out_var,
            DataChunk& output, idx_t index, idx_t row, idx_t col, idx_t count);

class FadeReaderFunction {
  public:
    static void FadeReaderImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> FadeReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<LocalTableFunctionState> FadeReaderInitLocal(ExecutionContext &context,
                          TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p);
    static unique_ptr<GlobalTableFunctionState> FadeReaderInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input);
};

} // namespace duckdb
