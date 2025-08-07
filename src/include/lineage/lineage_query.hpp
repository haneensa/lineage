#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/query_result.hpp"
#include "lineage/lineage_init.hpp"

namespace duckdb {

struct SourceContext {
  vector<idx_t> iids;
  // only if not nested, iid->oid
  vector<idx_t> zone_map;
  idx_t join_id;
  bool nested;
  SourceContext(idx_t join_id, bool nested) : join_id(join_id), nested(nested) {}
};

void LQ(idx_t qid, idx_t opid,
       vector<idx_t>& oids_per_partition,
        unordered_map<idx_t, vector<shared_ptr<SourceContext>>>& sources);

struct LQBindData : public TableFunctionData {
  idx_t oid;
  idx_t offset = 0;
};

struct LQGlobalState : public GlobalTableFunctionState {
    explicit LQGlobalState(unordered_map<idx_t, vector<shared_ptr<SourceContext>>> out_per_src_p)
      : out_per_src(std::move(out_per_src_p)), finished(false) {
        source_iter = out_per_src.begin();
    }

    unordered_map<idx_t, vector<shared_ptr<SourceContext>>> out_per_src;
    unordered_map<idx_t, vector<shared_ptr<SourceContext>>>::iterator source_iter;
    idx_t outer_cur;
    idx_t inner_offset;
    bool finished;
};


class LQFunction {
  public:
    static void LQImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    static unique_ptr<FunctionData> LQBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> LQInit(ClientContext &context,
                                                 TableFunctionInitInput &input);
};

} // namespace duckdb
