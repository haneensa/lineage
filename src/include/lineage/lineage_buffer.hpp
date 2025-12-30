#pragma once
#include "duckdb.hpp"
#include "lineage/lineage_plan.hpp"

namespace duckdb {


// Materialized copy of a DuckDB vector.
// Owns its memory and must be explicitly cleared.
struct IVector {
    bool is_valid = true;
    idx_t count = 0;
    sel_t *sel = nullptr;
    data_ptr_t data = nullptr;

    validity_t *validity = nullptr;    // bitmap of nulls (64 bits per entry)
    idx_t validity_bytes;    // size in bytes of validity mask
                             
    void clear() {
      if (sel) {
        free(sel);
        sel = nullptr;
      }
      if (data) {
        free(data);
        data = nullptr;
      }
      if (validity) {
        free(validity);
        validity = nullptr;
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
  idx_t fill_list_lineage(vector<vector<idx_t>>& lineage2D);
  void fill_local(vector<vector<IVector>>& store, vector<idx_t>& lineage1D);
  idx_t get_total_count();
  void clear() {
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
  }
};


void LogVector(Vector &vec, idx_t count, IVector &entry);
void LogListBigIntVector(Vector &vec, idx_t count, IVector &entry);

void PrintLoggedVector(const IVector &entry, idx_t type_size);

} // namespace duckdb
