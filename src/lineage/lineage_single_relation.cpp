#include "lineage/lineage_init.hpp"
#include <iostream>

namespace duckdb {
// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

static vector<idx_t> RemapLineage(const vector<idx_t> &parent_idx,
             const vector<idx_t> &mapping) {

    const idx_t n = parent_idx.empty()
                        ? mapping.size()
                        : parent_idx.size();

    vector<idx_t> result(n);

    if (parent_idx.empty()) {
        for (idx_t i = 0; i < n; ++i)
            result[i] = mapping[i];
    } else {
        for (idx_t i = 0; i < n; ++i)
            result[i] = mapping[parent_idx[i]];
    }

    return result;
}


void CreateJoinAggBlocks(idx_t qid, idx_t opid,
                        vector<JoinAggBlocks>& lineage_blocks,
                        vector<idx_t> lineage_idx, idx_t count) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  string table = to_string(qid) + "_" + to_string(opid);

  LDEBUG("creaeJoinAggBlocks ", table,
    " ", EnumUtil::ToChars<LogicalOperatorType>(lop_info->type));
  switch (lop_info->type) {

   // ----------------------------------------------------
   // Leaf scan
   // ----------------------------------------------------
   case LogicalOperatorType::LOGICAL_GET: {
      lineage_blocks.back().n = lineage_idx.size();
      // need to assert all sources for the block have the same size
      lineage_blocks.back().srcs_lineage[opid] = std::move(lineage_idx);
      return;
   }


   // ----------------------------------------------------
   // Leaf scan
   // ----------------------------------------------------
   case LogicalOperatorType::LOGICAL_PROJECTION:
   case LogicalOperatorType::LOGICAL_TOP_N:
   case LogicalOperatorType::LOGICAL_ORDER_BY:
   case LogicalOperatorType::LOGICAL_FILTER: {
    if (!lop_info->materializes_lineage) {
        CreateJoinAggBlocks(qid, lop_info->children[0],
                          lineage_blocks, lineage_idx, count);
        return;
     }

     vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
     idx_t n = lineage_idx.empty() ? glineage[0].size() : lineage_idx.size();

     auto new_idx = RemapLineage(lineage_idx, glineage[0]);
     CreateJoinAggBlocks(qid, lop_info->children[0],
                         lineage_blocks, std::move(new_idx), n);
     return;
   } 
   // ----------------------------------------------------
   // Join
   // ----------------------------------------------------
   case LogicalOperatorType::LOGICAL_DELIM_JOIN:
   case LogicalOperatorType::LOGICAL_ASOF_JOIN:
   case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
   case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
     vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
     idx_t n = lineage_idx.empty() ? glineage[0].size() : lineage_idx.size();
     auto left_idx = RemapLineage(lineage_idx, glineage[0]);
     CreateJoinAggBlocks(qid, lop_info->children[0], lineage_blocks, std::move(left_idx), n);
     if (lop_info->children.size() == 2) {
       auto right_idx = RemapLineage(lineage_idx, glineage[1]);
       CreateJoinAggBlocks(qid, lop_info->children[1], lineage_blocks, std::move(right_idx), n);
     }
     return;
   }
  
   // ----------------------------------------------------
   // Aggregate
   // ---------------------------------------------------- 
   case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
     // if aggs lineage not consequetive, then create a new lineage_block entry to fill
     vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
     lineage_blocks.back().has_agg = true;
     vector<idx_t>& forward_lineage = lineage_blocks.back().forward_lineage;
     vector<idx_t> new_lineage_idx;
     if (lineage_idx.empty()) {
       for (idx_t gid=0; gid < glineage.size(); ++gid) {
        for (idx_t i=0; i < glineage[gid].size(); ++i) {
          new_lineage_idx.push_back( glineage[gid][i] );
          forward_lineage.push_back(gid);
        }
       }
     } else {
       for (idx_t gid=0; gid < lineage_idx.size(); ++gid) {
        for (idx_t i=0; i < glineage[lineage_idx[gid]].size(); ++i) {
          new_lineage_idx.push_back( glineage[ lineage_idx[gid] ][i] );
          forward_lineage.push_back(gid);
        }
       }
     }
     idx_t cnt = new_lineage_idx.size();
     CreateJoinAggBlocks(qid, lop_info->children[0], lineage_blocks, std::move(new_lineage_idx), cnt);
     return;
   }
   default: {}}
}

} // namespace duckdb
