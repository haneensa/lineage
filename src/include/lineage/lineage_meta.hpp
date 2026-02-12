#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct BindData : public TableFunctionData {
  idx_t offset = 0;
};

class LineageMetaFunction {
  public:
    static void Implementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
};

} // namespace duckdb
