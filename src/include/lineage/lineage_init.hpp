#pragma once
#include "duckdb.hpp"

namespace duckdb {

// tree of lineage points (ignore pipelined operators because we pipeline their lineage)
struct LineageInfoNode {
  idx_t opid;
  int sink_id; // the first ancestor with has_lineage set or NULL if root
  vector<int> source_id; // the first child with has_lineage (extension) or leaf node
  LogicalOperatorType type;
  JoinType join_type;
  vector<idx_t> children;
  string table_name;
  vector<string> columns;
  bool has_lineage;
  bool delim_flipped;
  idx_t n_output;
  LineageInfoNode(idx_t opid, LogicalOperatorType type) : opid(opid), type(type),
  n_output(0), sink_id(-1), table_name(""), has_lineage(false), delim_flipped(false) {}
};

typedef unordered_map<idx_t, vector<Vector>> Payload;
typedef idx_t OPID;
typedef idx_t QID;
typedef string QID_OPID;
typedef string QID_OPID_TID;

struct ArtifactsLog {
  vector<std::pair<Vector, Vector>> agg_update_log;
  vector<std::pair<Vector, Vector>> agg_combine_log;
  vector<Vector> agg_finalize_log;
};


static std::atomic<idx_t> global_thread_counter{0};

inline idx_t GetThreadId() {
    // thread_local ensures one value per thread
    thread_local idx_t thread_id = global_thread_counter++;
    return thread_id;
}

struct LineageState {
   static bool cache;
   static bool capture;
   // used to disabale lineage capture for aggregates
   static bool hybrid;
   static bool persist;
   static bool use_internal_lineage;
   static bool debug;
   static std::unordered_map<QID_OPID, LogicalOperatorType> lineage_types;
   static std::unordered_map<QID_OPID, vector<std::pair<Vector, int>>> lineage_store;
   static std::unordered_map<QID_OPID, vector<vector<idx_t>>> lineage_global_store;
   static std::unordered_map<QID, unordered_map<OPID, unique_ptr<LineageInfoNode>>> qid_plans;
   static std::unordered_map<QID, OPID> qid_plans_roots;
   
   static std::mutex g_log_lock;
   static thread_local ArtifactsLog* active_log;
   static unordered_map<QID_OPID_TID, shared_ptr<ArtifactsLog>> logs;
};

unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                      unique_ptr<LogicalOperator>& plan);

bool IsSPJUA(unique_ptr<LogicalOperator>& plan);

} // namespace duckdb
