#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct LineageMetaBindData : public TableFunctionData {
  idx_t cardinality;
  idx_t chunk_count;

  void Initialize() {
    cardinality = 0;
    chunk_count = 0;
  }
};

class LineageMetaFunction {
  public:
    static void LineageMetaImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> LineageMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
};

} // namespace duckdb
