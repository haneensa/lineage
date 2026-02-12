#pragma once
#include "duckdb.hpp"
#include "lineage/lineage_plan.hpp"
#include "lineage/lineage_buffer.hpp"

#include <iostream>


namespace duckdb {

typedef idx_t OPID;
typedef idx_t QID;
typedef string QID_OPID;

static std::atomic<idx_t> global_thread_counter{0};
struct JoinAggBlocks {
  idx_t sink_opid; // --> root ; if join followed by agg, that is the end of a block and start of another block
  vector<idx_t> forward_lineage; // forward lineage if agg is between sink and src
  unordered_map<idx_t, vector<idx_t>> srcs_lineage;
  idx_t n;
  bool has_agg;
};

unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                      unique_ptr<LogicalOperator>& plan);

bool IsSPJUA(unique_ptr<LogicalOperator>& plan);

// construct global 1D/2D per-op lineage
// set |input| and |output| for each operators
idx_t InitGlobalLineageBuff(ClientContext& context, idx_t qid, idx_t opid);

void CreateJoinAggBlocks(idx_t qid, idx_t opid, vector<JoinAggBlocks>& lineage_blocks,
                        vector<idx_t> lineage_idx, idx_t lineage_idx_cnt);

struct LineageState {
   static bool capture;
   static bool persist;
   static bool debug;
   
   static std::unordered_map<QID_OPID, LogicalOperatorType> lineage_types;
   static std::unordered_map<QID, OPID> qid_plans_roots;
   static std::unordered_map<QID, unordered_map<OPID, unique_ptr<LineageInfoNode>>> qid_plans;

   // Maps logical operator pointers to ids.
   // Valid only during optimization / plan rewrite.
   static unordered_map<void*, idx_t> pointer_to_opid;
   
   static std::unordered_map<QID_OPID, vector<vector<idx_t>>> lineage_global_store;
   static unordered_map<string, unique_ptr<PartitionedLineage>> partitioned_store_buf;
   static unordered_map<QID, vector<JoinAggBlocks>> lineage_blocks;
   
   static std::mutex g_log_lock;
   static void Clear() {
      std::lock_guard<std::mutex> lock(g_log_lock);
      lineage_types.clear();
      qid_plans_roots.clear();
      qid_plans.clear();
      pointer_to_opid.clear();

      lineage_global_store.clear();
      for (auto& e : partitioned_store_buf) {
        e.second->clear();
      }
      partitioned_store_buf.clear();
   }
};

inline void LDebug(const string &msg) {
  if (LineageState::debug) {
    std::cout << "[DEBUG]" <<  msg << std::endl;
  }
}

inline string TypesToString(const vector<LogicalType>& types) {
  string types_str = "[";
  for (auto &typ : types) 
    types_str += typ.ToString() + ",";
  types_str += "]";
  return types_str;
}


} // namespace duckdb
