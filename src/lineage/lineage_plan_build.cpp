#include "lineage/lineage_plan.hpp"
#include "lineage/lineage_init.hpp"

#include <iostream>
#include <string>
#include <sstream>
#include <iomanip>  // for setw, setfill

#include "lineage/logical_lineage_operator.hpp"

#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

unordered_map<void*, idx_t> LineageState::pointer_to_opid;

// Phase 1: Logical plan annotation
//
// Traverses the logical operator tree and builds a parallel
// lineage-plan tree. At this stage, only structural information
// (operator type, children, scan metadata) is recorded.
//
// Lineage edges (source/sink relationships) are resolved in a
// later post-processing pass.
idx_t AnnotatePlan(unique_ptr<LogicalOperator> &op, idx_t query_id, idx_t& next_opid) {
  // Assign a stable operator id for this logical operator
  idx_t opid = next_opid++;

  // Create lineage node corresponding to this logical operator
  auto lop =  make_uniq<LineageInfoNode>(opid, op->type);

  // Recursively annotate children first (post-order traversal)
  for (auto& child : op->children) {
    idx_t child_opid = AnnotatePlan(child, query_id, next_opid);
    lop->children.push_back(child_opid);
  }

  // Capture scan metadata for base tables.
  if (op->type == LogicalOperatorType::LOGICAL_GET) {
    auto &get = op->Cast<LogicalGet>();
    if (get.GetTable()) {
      string table_name = get.GetTable()->name;
      lop->table_name = table_name;
      lop->columns = get.names;
    }
  }
  
  // Delim joins require special handling because their logical
  // children do not correspond 1:1 with lineage flow.
  if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    auto &join = op->Cast<LogicalComparisonJoin>();
    lop->delim_flipped = join.delim_flipped;
  }
  
  LineageState::qid_plans[query_id][opid] =  std::move(lop);
  LineageState::pointer_to_opid[(void*)op.get()] = opid;
  return opid;
}

// both the join and distinct take the same input (right delim dedup the right child, left the left child)
// RIGHT DELIM JOIN: join.children[1] -> delim.children[0] -> distinct
// delim.children[0] -> join.children[1]
// LEFT DELIM JOIN: join.children[0] -> delim.children[0] -> distinct
// join.children[0] -> ColumnDataScan -> join.children[0]
// 1) access to distinct to add LIST(rowid) expression
// 2) JOIN to add annotations from both sides
void LinkParentDelimGets(idx_t query_id, idx_t opid, idx_t source_opid) {
  // find delim_get, set its source_opid to join.children[1]
  // set delim_get source_opid to join.children[0]
  auto &lop = LineageState::qid_plans[query_id][opid];
  
  if (lop->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
    if (LineageState::debug) std::cout << "LogicalDelimGet opid: "
                             << opid << " src_opid: " << source_opid << std::endl;
    lop->source_opids.push_back(source_opid);
  }

  for (idx_t i=0; i < lop->children.size(); ++i) {
    idx_t child = lop->children[i];
    LinkParentDelimGets(query_id, child, source_opid);
  }
}

idx_t find_first_opid_with_lineage_or_leaf(idx_t query_id, idx_t opid) {
  auto &lop = LineageState::qid_plans[query_id][opid];
  if (lop->materializes_lineage) return opid; // lineage sink
  if (lop->children.empty()) return opid; // leaf
  return find_first_opid_with_lineage_or_leaf(query_id, lop->children[0]);
}

// Phase 2: Lineage edge resolution
//
// This pass resolves source and sink relationships between lineage nodes.
// For each operator that materializes lineage, we:
//   1) Identify its downstream lineage sources (children or leaves)
//   2) Identify its upstream sink (nearest ancestor with lineage)
//   3) Handle special operators (e.g., DELIM JOIN / DELIM GET)

// The traversal propagates the current sink id top-down.
// Resolves lineage source/sink relationships for a single operator.
//
// Inputs:
//   - current_sink: nearest ancestor that materializes lineage
//
// Outputs:
//   - node->sink_opid
//   - node->source_opidss
//   - next_sinks: sink ids to propagate to each child (by index)
//
// Invariant:
//   If the node materializes lineage, next_sinks.size() == node->children.size()
void ResolveLineageEdges(idx_t query_id,
                         idx_t opid,
                         idx_t current_sink,
                         vector<idx_t> &next_sinks) {
  auto &node = LineageState::qid_plans[query_id][opid];


  if (!node->materializes_lineage) {
    // Default: propagate current sink unchanged
    next_sinks.assign(node->children.size(), current_sink);
    return;
  }

  // This operator becomes a new lineage sink
  node->sink_opid = current_sink;

  // --- Special case: DELIM JOIN ---
  if (node->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    idx_t scan_child = node->delim_flipped ? 1 : 0;
    idx_t get_child  = node->delim_flipped ? 0 : 1;

    // Find the true lineage source on the scan side
    idx_t src =
        find_first_opid_with_lineage_or_leaf(query_id,
                                             node->children[scan_child]);

    // Wire DELIM GET operators back to the source
    LinkParentDelimGets(query_id,
                        node->children[get_child],
                        src);
  }

  // --- Determine lineage sources ---
  if (!node->children.empty()) {
    // Left child always contributes a lineage source
    idx_t left_src =
        find_first_opid_with_lineage_or_leaf(query_id,
                                             node->children[0]);
    node->source_opids.push_back(left_src);
  } else if (node->type != LogicalOperatorType::LOGICAL_DELIM_GET) {
    // leaf operator that materializes lineage
    node->source_opids.push_back(opid);
  } // else DELIM GET is handeled in LinkParentDelimGets()
    
  next_sinks.push_back(node->source_opids[0]);

  // Binary operators contribute a second lineage source
  if (node->children.size() > 1) {
    idx_t right_src =
        find_first_opid_with_lineage_or_leaf(query_id,
                                             node->children[1]);
    node->source_opids.push_back(right_src);
    next_sinks.push_back(right_src);
  }
}

// Recursively resolves lineage edges for the entire lineage plan.
void PostAnnotateRecursive(idx_t query_id,
                           idx_t opid,
                           idx_t current_sink) {
  auto &node = LineageState::qid_plans[query_id][opid];

  vector<idx_t> next_sinks;
  ResolveLineageEdges(query_id, opid, current_sink, next_sinks);

  // Recurse into children with appropriate sink context
  for (idx_t i = 0; i < node->children.size(); i++) {
    PostAnnotateRecursive(query_id,
                          node->children[i],
                          next_sinks[i]);
  }
}

void PostAnnotate(idx_t query_id, idx_t root) {
  idx_t initial_sink = DConstants::INVALID_INDEX;
  PostAnnotateRecursive(query_id, root, initial_sink);
}

std::string escape_json(const std::string &s) {
    std::ostringstream o;
    for (auto c = s.cbegin(); c != s.cend(); c++) {
        switch (*c) {
            case '"': o << "\\\""; break;
            case '\\': o << "\\\\"; break;
            case '\b': o << "\\b"; break;
            case '\f': o << "\\f"; break;
            case '\n': o << "\\n"; break;
            case '\r': o << "\\r"; break;
            case '\t': o << "\\t"; break;
            default:
                if ('\x00' <= *c && *c <= '\x1f') {
                    o << "\\u"
                      << std::hex << std::setw(4) << std::setfill('0') << (int)*c;
                } else {
                    o << *c;
                }
        }
    }
    return o.str();
}

std::string serialize_to_json(idx_t qid, idx_t root) {
  auto &lop = LineageState::qid_plans[qid][root];
  
  std::ostringstream oss;
  oss << "{";
  oss << "\"opid\": " << root << ",";
  oss << "\"name\": \"" << escape_json(EnumUtil::ToChars<LogicalOperatorType>(lop->type)) << "\",";
  oss << "\"sink_opid\": " << lop->sink_opid << ","; // the first ancestor with materializes_lineage set or NULL if root
  oss << "\"source_opids\": [";
  for (size_t i = 0; i < lop->source_opids.size(); i++) {
    if (i > 0) oss << ",";
    oss << lop->source_opids[i];
  }
  oss << "],"; // the first child with materializes_lineage (extension) or leaf node
  oss << "\"table\": \"" << escape_json(lop->table_name) << "\",";
  oss << "\"source_table\": [";
  for (size_t i = 0; i < lop->source_opids.size(); i++) {
    if (i > 0) oss << ",";
    oss << "\"";
    string src_table = LineageState::qid_plans[qid][lop->source_opids[i]]->table_name;
    oss << escape_json(src_table);
    oss << "\""; 
  }  
  oss <<  "],"; // source node table_name // if not scan then the op type
  oss << "\"materializes_lineage\": " << (lop->materializes_lineage ? "true" : "false") << ",";
  oss << "\"children\": [";
  for (size_t i = 0; i < lop->children.size(); i++) {
    if (i > 0) oss << ",";
    oss << serialize_to_json(qid, lop->children[i]);
  }
  oss << "]";
  oss << "}";
  
  return oss.str();
}

} // namespace duckdb
