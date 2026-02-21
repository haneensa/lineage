#pragma once
#include "duckdb.hpp"
#include "lineage/lineage_plan.hpp"

namespace duckdb {


// IVector is a materialized snapshot of a DuckDB Vector.
// It owns all underlying memory (malloc/free) and is detached
// from DuckDB's buffer manager and vector lifetimes.
//
// NOTE:
// - IVector is move-only to prevent double-free bugs.
// - All allocations MUST use malloc-compatible allocators.
struct IVector {
    bool is_valid = true;
    idx_t count = 0;
    sel_t *sel = nullptr;               // selection vector (owned)
    data_ptr_t data = nullptr;          // raw data buffer (owned)

    validity_t *validity = nullptr;    // bitmap of nulls (64 bits per entry, owned)
    idx_t validity_bytes;              // size in bytes of validity mask
                             
    idx_t bytes = 0;

    void clear() {
      free(sel); sel = nullptr;
      free(data); data = nullptr;
      free(validity); validity = nullptr;
      count = 0;
      validity_bytes = 0;
    }
};

// PartitionedLineage stores lineage buffers per execution partition.
// Used to support parallel joins and aggregations.
//
// Thread safety:
// - Ingestion is protected by p_lock
// - Read-only access is expected after finalization
struct PartitionedLineage {
  std::mutex p_lock;
  // Per-partition lineage buffers
  vector<vector<IVector>> left;
  vector<vector<IVector>> right;
  // Partition metadata
  vector<vector<idx_t>> zones;
  vector<vector<idx_t>> local_offsets;
  void fill_lineage(bool is_left, vector<idx_t>& lineage1D, idx_t total_count);
  idx_t fill_list_lineage(vector<vector<idx_t>>& lineage2D);
  void fill_local(vector<vector<IVector>>& store, vector<idx_t>& lineage1D);
  idx_t get_total_count();
  idx_t size();

  void clear() {
    std::lock_guard<std::mutex> lock(p_lock);
    for (auto& partition : left) {
      for (auto& v : partition) {
        v.clear();
      }
    }
    for (auto& partition : right) {
      for (auto& v : partition) {
        v.clear();
      }
    }

    left.clear();
    right.clear();
    zones.clear();
    local_offsets.clear();
  }
};


void LogVector(Vector &vec, idx_t count, IVector &entry);
void LogListBigIntVector(Vector &vec, idx_t count, IVector &entry);

void PrintLoggedVector(const IVector &entry, idx_t type_size);

} // namespace duckdb
