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


struct IVector {
    bool is_valid = true;
    idx_t count = 0;
    sel_t *sel = nullptr;
    data_ptr_t data = nullptr;

    validity_t *validity;    // bitmap of nulls (64 bits per entry)
    idx_t validity_bytes;    // size in bytes of validity mask

    IVector() = default;

    // Disable copy (prevents shallow copies)
    IVector(const IVector &) = delete;
    IVector &operator=(const IVector &) = delete;
      // Enable move
    IVector(IVector &&other) noexcept {
        is_valid = other.is_valid;
        count = other.count;
        sel = other.sel;
        data = other.data;

        other.sel = nullptr;
        other.data = nullptr;
        other.count = 0;
    }

      IVector &operator=(IVector &&other) noexcept {
        if (this != &other) {
            // Free existing memory
            if (sel) free(sel);
            if (data) free(data);

            // Steal resources
            is_valid = other.is_valid;
            count = other.count;
            sel = other.sel;
            data = other.data;

            // Null out the old one
            other.sel = nullptr;
            other.data = nullptr;
            other.count = 0;
            other.is_valid = true;
        }
        return *this;
    }

    ~IVector() {
      if (sel) {
        free(sel);
        sel = nullptr;
      }
      if (data) {
        free(data);
        data = nullptr;
      }
    }
};

struct PartitionedLineage {
  std::mutex p_lock;
  vector<vector<IVector>> left;
  vector<vector<IVector>> right;
  vector<vector<idx_t>> zones;
  vector<vector<idx_t>> local_offsets;
  void fill_lineage(bool is_left, vector<idx_t>& lineage1D, idx_t total_count);
  void fill_list_lineage(vector<vector<idx_t>>& lineage2D);
  void fill_local(vector<vector<IVector>>& store, vector<idx_t>& lineage1D);
  idx_t get_total_count();
};


struct ArtifactsLog {
  // copy vectors
  vector<std::pair<Vector, vector<Vector>>> agg_update_log;
  vector<std::pair<Vector, Vector>> agg_combine_log;
  vector<Vector> agg_finalize_log;
  // copy buffers
  vector<std::pair<IVector, vector<IVector>>> buffer_agg_update_log;
  vector<std::pair<IVector, IVector>> buffer_agg_combine_log;
  vector<std::pair<IVector, idx_t>> buffer_agg_finalize_log;
};

void LogVector(Vector &vec, idx_t count, IVector &entry);
void LogListBigIntVector(Vector &vec, idx_t count, IVector &entry);

static std::atomic<idx_t> global_thread_counter{0};

void PrintLoggedVector(const IVector &entry, idx_t type_size);

inline idx_t GetThreadId() {
    // thread_local ensures one value per thread
    thread_local idx_t thread_id = global_thread_counter++;
    return thread_id;
}

struct LineageState {
   static bool use_vector;
   static bool cache;
   static bool capture;
   // used to disabale lineage capture for aggregates
   static bool hybrid;
   static bool persist;
   static bool use_internal_lineage;
   static bool debug;
   static std::unordered_map<QID_OPID, LogicalOperatorType> lineage_types;
   static std::unordered_map<QID_OPID, vector<std::pair<Vector, int>>> lineage_store;
   
   static std::unordered_map<QID_OPID, vector<IVector>> lineage_store_buf;
   
   static std::unordered_map<QID_OPID, vector<vector<idx_t>>> lineage_global_store;
   static std::unordered_map<QID, unordered_map<OPID, unique_ptr<LineageInfoNode>>> qid_plans;
   static std::unordered_map<QID, OPID> qid_plans_roots;
   
   static std::mutex g_log_lock;
   static thread_local ArtifactsLog* active_log;
   static thread_local string active_log_key;
   static unordered_map<QID_OPID_TID, shared_ptr<ArtifactsLog>> logs;
   static unordered_map<string, shared_ptr<PartitionedLineage>> partitioned_store_buf;
};

unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                      unique_ptr<LogicalOperator>& plan);

bool IsSPJUA(unique_ptr<LogicalOperator>& plan);

struct LineageUDABindData : public FunctionData {
	LogicalType return_type;
  idx_t operator_id;
  std::atomic<idx_t> g_offset;
  std::mutex g_lock;

	explicit LineageUDABindData(LogicalType return_type_p, idx_t op_id)
	    : return_type(std::move(return_type_p)), operator_id(op_id), g_offset(0) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<LineageUDABindData>(return_type, operator_id);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LineageUDABindData>();
		return return_type == other.return_type && operator_id == other.operator_id;
	}
};



} // namespace duckdb
