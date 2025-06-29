#include "lineage/lineage_init.hpp"

#include <iostream>

#include "lineage_extension.hpp"
#include "lineage/logical_lineage_operator.hpp"

#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

unordered_map<void*, idx_t> pointer_to_opid;

bool IsSPJUA(unique_ptr<LogicalOperator>& plan) {
  if ( (plan->type == LogicalOperatorType::LOGICAL_PROJECTION
      || plan->type == LogicalOperatorType::LOGICAL_ORDER_BY
      || plan->type == LogicalOperatorType::LOGICAL_GET
      || plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN
      || plan->type == LogicalOperatorType::LOGICAL_CHUNK_GET
      || plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE
      || plan->type == LogicalOperatorType::LOGICAL_CTE_REF
      || plan->type == LogicalOperatorType::LOGICAL_FILTER
      || plan->type == LogicalOperatorType::LOGICAL_TOP_N
      || plan->type == LogicalOperatorType::LOGICAL_ASOF_JOIN
      || plan->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT
      || plan->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY
      || plan->type == LogicalOperatorType::LOGICAL_DELIM_GET
      || plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN
      ) != true) return false;
  
  for (auto &child : plan->children) {
    if (IsSPJUA(child) == false) return false;
  }

  return true;
}

AggregateFunction GetListFunction(ClientContext &context) {
  auto &catalog = Catalog::GetSystemCatalog(context);
  auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
      context, DEFAULT_SCHEMA, "list"
  );
  return entry.functions.GetFunctionByArguments(context, {LogicalType::ROW_TYPE});
}

idx_t ProcessJoin(unique_ptr<LogicalOperator> &op, vector<idx_t>& rowids, idx_t query_id, idx_t& cur_op_id) {
  auto &join = op->Cast<LogicalComparisonJoin>();
  idx_t left_col_id = 0;
  idx_t right_col_id = 0;
  
  if (join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
    if (LineageState::debug)
    std::cout << "inject right semi join: " << rowids[1] << " " << join.right_projection_map.size() << std::endl;
    if (!join.right_projection_map.empty()) {
      right_col_id = join.right_projection_map.size();
      join.right_projection_map.push_back(rowids[1]);
    } else {
      right_col_id = rowids[1];
    }

    if (LineageState::debug)
    std::cout << "-> " << left_col_id + right_col_id << " " << left_col_id << " " << right_col_id << " " << rowids[0] << " " << rowids[1] << " "
      << join.left_projection_map.size() << " " << join.right_projection_map.size() << std::endl;
    int source_count = 1;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id,
        query_id, op->type, source_count,  0, right_col_id);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return right_col_id;
  }

  if (!join.left_projection_map.empty()) {
    left_col_id = join.left_projection_map.size();
    join.left_projection_map.push_back(rowids[0]);
  } else {
    left_col_id = rowids[0];
  }
  
   if (join.join_type == JoinType::SEMI
          || join.join_type == JoinType::ANTI
          || join.join_type == JoinType::MARK) {
      if (LineageState::debug)
    std::cout << "inject shortcut join: " << left_col_id << " " << join.left_projection_map.size() << std::endl;
    int source_count = 1;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
        op->type, source_count, left_col_id, 0);
    lop->AddChild(std::move(op));
    if (join.join_type == JoinType::MARK) {
      lop->mark_join = true;
      left_col_id++; /* bool col */
    }
    op = std::move(lop);
    return left_col_id;
  }

  if (!join.right_projection_map.empty()) {
    right_col_id = join.right_projection_map.size();
    join.right_projection_map.push_back(rowids[1]);
  } else {
    right_col_id = rowids[1];
  }

  if (LineageState::debug)  std::cout << "[DEBUG] Join: " << EnumUtil::ToChars<JoinType>(join.join_type) <<
      "-> " << left_col_id + right_col_id << " " << left_col_id << " " << right_col_id
      << " " << join.left_projection_map.size() << " "
      << join.right_projection_map.size() << std::endl;

  int source_count = 2;
  auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
      op->type, source_count, left_col_id, right_col_id);
  lop->AddChild(std::move(op));
  op = std::move(lop);
  return left_col_id + right_col_id;
}


idx_t AnnotatePlan(unique_ptr<LogicalOperator> &op, idx_t query_id, idx_t& ref_opid) {
  idx_t opid = ref_opid++;
  auto lop =  make_uniq<LineageInfoNode>(opid, op->type);

  for (auto& child : op->children) {
    idx_t child_idx = AnnotatePlan(child, query_id, ref_opid);
    lop->children.push_back(child_idx);
  }

  if (op->type == LogicalOperatorType::LOGICAL_GET) {
    auto &get = op->Cast<LogicalGet>();
    if (get.GetTable()) {
      string table_name = get.GetTable()->name;
      lop->table_name = table_name;
      lop->columns = get.names;
    }
  }
  
  LineageState::qid_plans[query_id][opid] =  std::move(lop);
  pointer_to_opid[(void*)op.get()] = opid;
  return opid;
}
  
idx_t find_first_opid_with_lineage_or_leaf(idx_t query_id, idx_t opid) {
  auto &lop = LineageState::qid_plans[query_id][opid];
  if (lop->has_lineage) return opid; // lineage sink
  if (lop->children.empty()) return opid; // leaf
  return find_first_opid_with_lineage_or_leaf(query_id, lop->children[0]);
}

void PostAnnotate(idx_t query_id, idx_t root, int &sink_id) {
  auto &lop = LineageState::qid_plans[query_id][root];

  vector<int> parents;
  if (lop->has_lineage) {
    if (lop->children.empty() == false) {
      idx_t src_id = find_first_opid_with_lineage_or_leaf(query_id, lop->children[0]);
      lop->source_id.push_back(src_id);
    } else { 
      lop->source_id.push_back(root);
    }
    lop->sink_id = sink_id;
    parents.push_back(lop->source_id[0]);

    if (lop->children.size() > 1) {
      idx_t src_id = find_first_opid_with_lineage_or_leaf(query_id, lop->children[1]);
      lop->source_id.push_back(src_id);
      parents.push_back(src_id);
    }
  }

  for (idx_t i=0; i < lop->children.size(); ++i) {
    idx_t child = lop->children[i];
    if (lop->has_lineage) sink_id = parents[i];
    PostAnnotate(query_id, child, sink_id);
  }
}

idx_t InjectLineageOperator(unique_ptr<LogicalOperator> &op,ClientContext &context, idx_t query_id) {
  if (!op) return 0;
  vector<idx_t> rowids = {};

  idx_t cur_op_id = pointer_to_opid[(void*)op.get()];

  for (auto &child : op->children) {
    rowids.push_back( InjectLineageOperator(child, context,  query_id) );
  }

  if (LineageState::debug) {
    std::cout << "[DEBUG] InjectLineageOperator: " << EnumUtil::ToChars<LogicalOperatorType>(op->type) <<
      " " << op->GetName() << ", op_id: " << cur_op_id << ", rowids: ";
    for (int i = 0; i < rowids.size(); ++i)  std::cout << " -> " << rowids[i];
    std::cout << std::endl;

  }
  if (op->type == LogicalOperatorType::LOGICAL_GET) {
    // leaf node. add rowid attribute to propagate.
    auto &get = op->Cast<LogicalGet>();
    get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
    //LineageState::table_idx = get.table_index;
    idx_t col_id =  get.GetColumnIds().size() - 1;
    // projection_ids index into column_ids. if any exist then reference new column
    if (!get.projection_ids.empty()) {
      get.projection_ids.push_back(col_id);
      col_id = get.projection_ids.size() - 1;
    }
    return col_id;
  }  else if (op->type == LogicalOperatorType::LOGICAL_CHUNK_GET) { // CTE_SCAN too
    // add lineage op to generate ids
    auto& col = op->Cast<LogicalColumnDataGet>();
    idx_t col_id = col.chunk_types.size();
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
        op->type, 1/*src cnt*/, col_id, 0);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return col_id;
  }  else if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) { // CTE_SCAN too
      return rowids[1];
  }  else if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) { // CTE_SCAN too
    // refrence a recursive CTE. table_index, cte_index, chunk_types, bound_columns
    // add lineage op to generate ids
    auto& col = op->Cast<LogicalCTERef>();
    idx_t col_id = col.chunk_types.size();
    if (LineageState::debug) std::cout << "[DEBUG] CETRef: " << col_id << " " << col.bound_columns[0] << " " << std::endl;
    col.chunk_types.push_back(LogicalType::ROW_TYPE);
    col.bound_columns.push_back("rowid");
    return col_id;
  } else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
    auto &filter = op->Cast<LogicalFilter>();
    int col_id = rowids[0];
    int new_col_id = col_id;
    if (op->children[0]->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
        // check if the child is mark join
        if (op->children[0]->Cast<LogicalLineageOperator>().mark_join) {
            // pull up lineage op
            auto lop = std::move(op->children[0]);
            if (LineageState::debug)
              std::cout << "pull up lineage op " << rowids[0] << " " 
            << filter.expressions.size() << " " << filter.projection_map.size() << " " << 
            lop->Cast<LogicalLineageOperator>().left_rid << std::endl;
            lop->Cast<LogicalLineageOperator>().dependent_type = op->type;
      
            idx_t child_left_rid = lop->Cast<LogicalLineageOperator>().left_rid;
            if (!filter.projection_map.empty()) {
              // annotations, but projection_map refer to the extra bool column that we need to adjust
              // the last column is the boolean
              filter.projection_map.push_back(child_left_rid);
              lop->Cast<LogicalLineageOperator>().left_rid = filter.projection_map.size()-1; 
              new_col_id = filter.projection_map.size()-1; 
              if (LineageState::debug) {
              for (idx_t i=0; i < filter.projection_map.size(); i++)
                std::cout << i << " -> " << filter.projection_map[i] << std::endl;

                std::cout << child_left_rid
                      << " " << lop->Cast<LogicalLineageOperator>().left_rid
                      << " " << new_col_id <<  std::endl;
              }
            }
            op->children[0] = std::move(lop->children[0]);
            lop->children[0] = std::move(op);
            op = std::move(lop);
            return new_col_id;
        }
    }
    if (!filter.projection_map.empty()) {
        filter.projection_map.push_back(col_id); 
        new_col_id = filter.projection_map.size()-1; 
    }
    if (LineageState::debug)  std::cout << "[DEBUG] Filter " << col_id << " " << new_col_id << std::endl;
    return new_col_id;
  } else if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
    // it passes through child types. except if projections is not empty, then we need to add it
    auto &order = op->Cast<LogicalOrder>();
    if (!order.projection_map.empty()) {
     // order.projections.push_back(); the rowid of child
    }
    return rowids[0];
  } else if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
    // passes through child types
    return rowids[0];
  } else if (op->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
    if (rowids.size() > 0)  return rowids[0];
    else return 0;
  } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
    // projection, just make sure we propagate any annotation columns
    int col_id = rowids[0];
    op->expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, col_id));
    int new_col_id = op->expressions.size()-1;
    return new_col_id;
  } else if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
    // duplicate eliminated scan (output of distinct)
    auto &get = op->Cast<LogicalDelimGet>();
    if (LineageState::debug)
      std::cout << "LogicalDelimGet types after injection: " << get.table_index << " " << get.chunk_types.size() << std::endl;
    int col_id = get.chunk_types.size();
    get.chunk_types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
        op->type, 1, col_id, 0);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return col_id; // TODO: adjust once I adjust distinct types
  } else if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    // the JOIN right child, becomes right_delim_join child that is used as input to
    // JOIN and DISTINCT
    // 1) access to distinct to add LIST(rowid) expression
    // 2) JOIN to add annotations from both sides
    // the fist n childrens are n delim scans
    return ProcessJoin(op, rowids, query_id, cur_op_id);
  } else if (op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
  } else if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
  } else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
    // Propagate annotations from the left and right sides.
    // Add PhysicaLineage to extraxt the last two columns
    // and replace it with a single annotations column
    return ProcessJoin(op, rowids, query_id, cur_op_id);
  } else if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
      // check if there is a filter between this and last lineageop or leaf node
      auto &aggr = op->Cast<LogicalAggregate>();
     // if (!aggr.groups.empty()) {
          auto list_function = GetListFunction(context);
          auto rowid_colref = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::ROW_TYPE, rowids[0]);
          vector<unique_ptr<Expression>> children;
          children.push_back(std::move(rowid_colref));
          unique_ptr<FunctionData> bind_info = list_function.bind(context, list_function, children);
          auto list_aggregate = make_uniq<BoundAggregateExpression>(
              list_function, std::move(children), nullptr, std::move(bind_info),
              AggregateType::NON_DISTINCT
          );

          aggr.expressions.push_back(std::move(list_aggregate));
          idx_t new_col_id = aggr.groups.size() + aggr.expressions.size() + aggr.grouping_functions.size() - 1;
          
          auto dummy = make_uniq<LogicalLineageOperator>(aggr.estimated_cardinality, cur_op_id, query_id,
              op->type, 1, new_col_id, 0);
          dummy->AddChild(std::move(op));

          op = std::move(dummy);
          
          return new_col_id;
    //  } // if simple agg, add operator below to remove annotations, and operator above to generate annotations
  }
  return 0;
}

unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                    unique_ptr<LogicalOperator>& plan) {
  idx_t query_id = LineageState::qid_plans_roots.size();
  idx_t cur_op_id = 0;
  AnnotatePlan(plan, query_id, cur_op_id);
  idx_t final_rowid = InjectLineageOperator(plan, input.context, query_id);
  // inject lineage op at the root of the plan to extract any annotation columns
  // If root is create table, then add lineage operator below it
  if (LineageState::debug) std::cout << "Annotation Column: " << final_rowid << ", Operator Id: " << cur_op_id << std::endl;
  idx_t root_id = pointer_to_opid[(void*)plan->children[0].get()];
  auto root = make_uniq<LogicalLineageOperator>(plan->estimated_cardinality,
      root_id, query_id, plan->children[0]->type, 1/*src_cnt*/, final_rowid, 0, true);
  LineageState::qid_plans_roots[query_id] = root->operator_id;
  if (plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
    auto child = std::move(plan->children[0]);
    root->AddChild(std::move(child));
    plan->children[0] = std::move(root);
  } else {
    root->AddChild(std::move(plan));
    plan = std::move(root);
  }
  
  int sink_id = -1;
  PostAnnotate(query_id, root_id, sink_id);
  pointer_to_opid.clear();

  return std::move(plan);
}

} // namespace duckdb
