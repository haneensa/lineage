#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {


// TODO: add generic reader that takes alloc_var and return the actual values
/*template<>
void IReader<fname>(DataChunk& output, idx_t row, idx_t col, idx_t count);
*/
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
