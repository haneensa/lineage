//===----------------------------------------------------------------------===//
//                         DuckDB
//
// TODO: move to inside duckdb
// duckdb/execution/lineage_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string.hpp"
#include <iostream>

namespace duckdb {

struct int_address_artifact {
  int_address_artifact(int* addresses, idx_t count)
  : addresses(addresses), count(count) {}
	int* addresses;
	idx_t count;
};

struct no_address_artifact {
  no_address_artifact(data_ptr_t* addresses, idx_t count)
  : addresses(addresses), count(count) {}
	data_ptr_t* addresses;
	idx_t count;
};

struct combine_artifact {
  combine_artifact(data_ptr_t* src, data_ptr_t* target, idx_t count)
  : src(src), target(target), count(count) {}
	data_ptr_t* src;
	data_ptr_t* target;
	idx_t count;
};

struct address_artifact {
  address_artifact(data_ptr_t* addresses, idx_t count)
    : addresses(addresses), count(count) {}
	data_ptr_t* addresses;
	idx_t count;
};

struct Artifacts {
  idx_t tuple_size;
  uintptr_t fixed;
	vector<int_address_artifact> int_scatter_log;
  vector<no_address_artifact> scatter_log;
	vector<combine_artifact> combine_log;
	vector<address_artifact> finalize_states_log;
  void clear() {
    // std::cout << "Clear Artifacts" << std::endl;
    for (int i=0; i < int_scatter_log.size(); ++i) {
      delete int_scatter_log[i].addresses;
    }
    for (int i=0; i < scatter_log.size(); ++i) {
      delete scatter_log[i].addresses;
    }
    for (int i=0; i < finalize_states_log.size(); ++i) {
      delete finalize_states_log[i].addresses;
    }
    for (int i=0; i < combine_log.size(); ++i) {
      delete combine_log[i].src;
      delete combine_log[i].target;
    }
    int_scatter_log.clear();
    scatter_log.clear();
    finalize_states_log.clear();
    combine_log.clear();
  }

  ~Artifacts() {
  }
};

class LineageManager {
public:
  LineageManager() {}
  void clear() {}
  bool capture;
};


struct LineageGlobal {
  static string explicit_join_type;
  static string  explicit_agg_type;
  static bool enable_filter_pushdown;
  static thread_local Artifacts a;
  static LineageManager LS;
};


} // namespace duckdb
