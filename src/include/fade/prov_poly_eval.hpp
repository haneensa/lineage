#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

class PolyEvalFunction {
  public:
    static void PolyEvalImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> PolyEvalBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> PolyEvalInit(ClientContext &context,
                                                 TableFunctionInitInput &input);
};

} // namespace duckdb
