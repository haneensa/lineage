/*
Relational view of query level lineage using SQL of operator level lineage
*/
#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/query_result.hpp"
 
namespace duckdb {

struct LineageGBindData : public TableFunctionData {
  idx_t offset = 0;
};

struct LineageGGlobalState : public GlobalTableFunctionState {
    explicit LineageGGlobalState(unique_ptr<QueryResult> result) : result(std::move(result)), finished(false) {
    }

    unique_ptr<QueryResult> result;
    bool finished;
};


class LineageGFunction {
  public:
    static void LineageGImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> LineageGBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> LineageGInit(ClientContext &context,
                                                 TableFunctionInitInput &input);
};

} // namespace duckdb
