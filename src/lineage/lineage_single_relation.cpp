#include "lineage/lineage_init.hpp"
#include <iostream>

namespace duckdb {

void CreateJoinAggBlocks(idx_t qid, idx_t opid, vector<JoinAggBlocks>& lineage_blocks,
                        vector<idx_t> lineage_idx, idx_t count) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  string table = to_string(qid) + "_" + to_string(opid);
 // std::cout << "prior: " << table << " " << lineage_blocks.size() << " " << lineage_idx.size() << " "
   //         << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << " " << count << std::endl;
  switch (lop_info->type) {
   case LogicalOperatorType::LOGICAL_GET: {
     // std::cout << " Logical Get: " << opid << " " << lineage_idx.size() << " " << count << std::endl;
      lineage_blocks.back().srcs_lineage[opid] = std::move(lineage_idx);
      break;
   }
   case LogicalOperatorType::LOGICAL_PROJECTION:
   case LogicalOperatorType::LOGICAL_TOP_N:
   case LogicalOperatorType::LOGICAL_ORDER_BY:
   case LogicalOperatorType::LOGICAL_FILTER: {
      if (!lop_info->materializes_lineage) {
          CreateJoinAggBlocks(qid, lop_info->children[0],
                               lineage_blocks, lineage_idx, count);
         // std::cout << "post: " << table << " " << lineage_blocks.size() 
         //   << " " << lineage_idx.size() << " "
         //   << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << std::endl;
          return;
      }
     vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
     // assert(glineage[0].size() == n);
     idx_t n = lineage_idx.empty() ? glineage[0].size() : lineage_idx.size();
     vector<idx_t> new_lineage_idx(n);
     if (lineage_idx.empty()) {
       for (idx_t i=0; i < n; ++i) new_lineage_idx[i] = glineage[0][i];
     } else {
       for (idx_t i=0; i < n; ++i) new_lineage_idx[i] = glineage[0][ lineage_idx[i] ];
     }
     CreateJoinAggBlocks(qid, lop_info->children[0],
                         lineage_blocks, new_lineage_idx, count);
      break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
     vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
     // assert(glineage[0].size() == n);
     idx_t n = lineage_idx.empty() ? glineage[0].size() : lineage_idx.size();
     vector<idx_t> left_lineage_idx(n);
     if (lineage_idx.empty()) {
       for (idx_t i=0; i < n; ++i) left_lineage_idx[i] = glineage[0][i];
     } else {
       for (idx_t i=0; i < n; ++i) left_lineage_idx[i] = glineage[0][ lineage_idx[i] ];
     }
     CreateJoinAggBlocks(qid, lop_info->children[0], lineage_blocks, left_lineage_idx, count);
     if (lop_info->children.size() == 2) {
       vector<idx_t> right_lineage_idx(n);
       if (lineage_idx.empty()) {
         for (idx_t i=0; i < n; ++i)  right_lineage_idx[i] = glineage[1][i];
       } else {
         for (idx_t i=0; i < n; ++i)  right_lineage_idx[i] = glineage[1][ lineage_idx[i] ];
       }
       CreateJoinAggBlocks(qid, lop_info->children[1], lineage_blocks, right_lineage_idx, count);
     }
      break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
     // if aggs lineage not consequetive, then create a new lineage_block entry to fill
     vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
     lineage_blocks.back().has_agg = true;
     // if sink_lineage == nullptr then use sequential ids
     // else, use sink_lineage to find the new group ids
     // lineage_blocks.back().forward_meta = table + _forward;
     vector<idx_t>& forward_lineage = lineage_blocks.back().forward_lineage;
     // std::cout << "glineage.size(): " << glineage.size() << " n_input: " << lop_info->n_input << " n_output:" << 
     //   lop_info->n_output << " " << std::endl;
     // lineage_idx = forward_lineage
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
     lineage_blocks.back().n = new_lineage_idx.size(); // new_lineage_idx becuase top ops could filter
     // std::cout << lop_info->n_input << " <> " << new_lineage_idx.size() << std::endl;
     CreateJoinAggBlocks(qid, lop_info->children[0], lineage_blocks, new_lineage_idx, new_lineage_idx.size());
     break;
   } default: {}}
  
  //std::cout << "post: " << table << " " << lineage_blocks.size() << " " << lineage_idx.size() << " "
    //        << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << std::endl;
}

} // namespace duckdb
