//===----------------------------------------------------------------------===//
//                         DuckDB
//
// TODO: move to inside duckdb
// duckdb/execution/lineage_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

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
  static LineageManager LS;
};


} // namespace duckdb
