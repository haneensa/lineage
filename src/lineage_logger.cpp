// TODO: move to inside duckdb
// #include "duckdb/execution/lineage_logger.hpp"
#include "lineage_logger.hpp"

namespace duckdb {

string LineageGlobal::explicit_join_type;
string LineageGlobal::explicit_agg_type;
bool LineageGlobal::enable_filter_pushdown;
LineageManager LineageGlobal::LS;

} // namespace duckdb
