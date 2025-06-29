#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct LineageMetaBindData : public TableFunctionData {
  idx_t offset = 0;
};

class LineageMetaFunction {
  public:
    static void LineageMetaImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> LineageMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
};

} // namespace duckdb
