#pragma once

#include "fade/fade.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class GetPredicatesFunction {
  public:
    static void GetPredicatesImpl(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> GetPredicatesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<LocalTableFunctionState> GetPredicatesInitLocal(ExecutionContext &context,
                          TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p);
};

} // namespace duckdb
