#include "lineage/lineage_init.hpp"

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
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

bool IsSPJUA(unique_ptr<LogicalOperator>& plan) {
  if ( (plan->type == LogicalOperatorType::LOGICAL_PROJECTION
      || plan->type == LogicalOperatorType::LOGICAL_UNION
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

AggregateFunction GetAggFunction(ClientContext &context, string fname) {
  auto &catalog = Catalog::GetSystemCatalog(context);
  auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
      context, DEFAULT_SCHEMA, fname
  );
  return entry.functions.GetFunctionByArguments(context, {LogicalType::ROW_TYPE});
}

idx_t HandleAggregate(unique_ptr<LogicalOperator> &op,
                      ClientContext &context,
                      vector<idx_t> &rowids,
                      idx_t query_id,
                      idx_t cur_op_id) {
    auto &aggr = op->Cast<LogicalAggregate>();
    // if (!aggr.groups.empty()) {
    //  TODO: if simple agg, add operator below to remove annotations, and operator above to generate annotations
    // }
    string fname = "list";
    auto list_function = GetAggFunction(context, fname);
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
}

idx_t HandleFilter(unique_ptr<LogicalOperator> &op, vector<idx_t> &rowids) {
  auto &filter = op->Cast<LogicalFilter>();
  int col_id = rowids[0];
  int new_col_id = col_id;
  if (op->children[0]->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
      if (op->children[0]->Cast<LogicalLineageOperator>().mark_join) { // pull up lineage op
          auto lop = std::move(op->children[0]);
          LDEBUG("pull up lineage op:", rowids[0], " ", filter.expressions.size(), " ",
              filter.projection_map.size(), " ", lop->Cast<LogicalLineageOperator>().left_rid) ;

          lop->Cast<LogicalLineageOperator>().dependent_type = op->type;
          idx_t child_left_rid = lop->Cast<LogicalLineageOperator>().left_rid;
          if (!filter.projection_map.empty()) {
            // annotations, but projection_map refer to the extra bool column that we need to adjust
            // the last column is the boolean
            filter.projection_map.push_back(child_left_rid);
            lop->Cast<LogicalLineageOperator>().left_rid = filter.projection_map.size()-1; 
            new_col_id = filter.projection_map.size()-1; 
            LDEBUG(child_left_rid, " ",  lop->Cast<LogicalLineageOperator>().left_rid, " ", new_col_id,
                  MapToString(filter.projection_map));
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
  return new_col_id;
}

idx_t HandleRightSemiJoin(unique_ptr<LogicalOperator> &op,
                 vector<idx_t> &rowids,
                 idx_t query_id,
                 idx_t cur_op_id) {
  auto &join = op->Cast<LogicalComparisonJoin>();
  idx_t right_col_id = 0;

  if (!join.right_projection_map.empty()) {
    right_col_id = join.right_projection_map.size();
    join.right_projection_map.push_back(rowids[1]);
  } else {
    right_col_id = rowids[1];
  }

  LDEBUG("HandleRightSemiJoin: ",  right_col_id, " ", rowids[0], " ", rowids[1],
        " ", join.left_projection_map.size(), " ", join.right_projection_map.size());
  const int source_count = 1;
  auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id,
                                              query_id, op->type, source_count,  0, right_col_id);
  lop->AddChild(std::move(op));
  op = std::move(lop);
  return right_col_id;
}

idx_t HandleLeftSemiJoin(unique_ptr<LogicalOperator> &op,
                 vector<idx_t> &rowids,
                 idx_t query_id,
                 idx_t cur_op_id) {
  auto &join = op->Cast<LogicalComparisonJoin>();
  idx_t left_col_id = 0;
  idx_t right_col_id = 0;
  
  if (!join.left_projection_map.empty()) {
    left_col_id = join.left_projection_map.size();
    join.left_projection_map.push_back(rowids[0]);
  } else {
    left_col_id = rowids[0];
  }
  
  LDEBUG("HandleLeftSemiJoin: ", left_col_id,  " ", join.left_projection_map.size());
  const int source_count = 1;
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

idx_t HandleRegularJoin(unique_ptr<LogicalOperator> &op,
                 vector<idx_t> &rowids,
                 idx_t query_id,
                 idx_t cur_op_id) {
  auto &join = op->Cast<LogicalComparisonJoin>();
  idx_t left_col_id = 0;
  idx_t right_col_id = 0;
  
  if (!join.left_projection_map.empty()) {
    left_col_id = join.left_projection_map.size();
    join.left_projection_map.push_back(rowids[0]);
  } else {
    left_col_id = rowids[0];
  }

  if (!join.right_projection_map.empty()) {
    right_col_id = join.right_projection_map.size();
    join.right_projection_map.push_back(rowids[1]);
  } else {
    right_col_id = rowids[1];
  }

  LDEBUG("HandleRegularJoin: ", EnumUtil::ToChars<JoinType>(join.join_type),
        " ", left_col_id + right_col_id, " ", left_col_id, " ", right_col_id,
        " ", join.left_projection_map.size(),
        " ", join.right_projection_map.size());

  const int source_count = 2;
  auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
      op->type, source_count, left_col_id, right_col_id);
  lop->AddChild(std::move(op));
  op = std::move(lop);
  return left_col_id + right_col_id;
}


idx_t HandleJoin(unique_ptr<LogicalOperator> &op,
                 vector<idx_t> &rowids,
                 idx_t query_id,
                 idx_t cur_op_id) {
    auto &join = op->Cast<LogicalComparisonJoin>();
    // Propagate annotations from the left and right sides.
    // Add PhysicaLineage to extraxt the last two columns
    // and replace it with a single annotations column
    auto& lop = LineageState::qid_plans[query_id][cur_op_id];
    lop->join_type = join.join_type;

    switch (join.join_type) {
        case JoinType::RIGHT_SEMI:
        case JoinType::RIGHT_ANTI:
            return HandleRightSemiJoin(op, rowids, query_id, cur_op_id);

        case JoinType::SEMI:
        case JoinType::ANTI:
        case JoinType::MARK:
            return HandleLeftSemiJoin(op, rowids, query_id, cur_op_id);

        default:
            return HandleRegularJoin(op, rowids, query_id, cur_op_id);
    }
}

idx_t HandleGet(unique_ptr<LogicalOperator> &op) {
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
}

idx_t HandleUnion(unique_ptr<LogicalOperator> &op, idx_t cur_op_id, idx_t query_id,
                  vector<idx_t>& rowids) {
  // extract lineage from left side, and right side
  // replace it with new rowid
  idx_t left_col_id = rowids[0];
  idx_t right_col_id = 0; //rowids[1];
  LDEBUG("UNION: ",left_col_id, " ", right_col_id);
  const int source_count = 1;
  auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
      op->type, source_count, left_col_id, 0);
  lop->AddChild(std::move(op));
  op = std::move(lop);

  // add lop below each child that append 0 or 1 to rowid  to indicate which source
  // it belongs to.

  // for the group by, exclude rowid from the groups, and add list aggregate
  return left_col_id;
}

idx_t HandleOperator(unique_ptr<LogicalOperator> &op,
                     ClientContext &context,
                     idx_t query_id,
                     vector<idx_t> &rowids,
                     idx_t cur_op_id) {
  switch (op->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      return HandleGet(op);
    } case LogicalOperatorType::LOGICAL_CHUNK_GET: { // CTE_SCAN too
      // add lineage op to generate ids
      auto& col = op->Cast<LogicalColumnDataGet>();
      idx_t col_id = col.chunk_types.size();
      auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
          op->type, 1/*src cnt*/, col_id, 0);
      lop->AddChild(std::move(op));
      op = std::move(lop);
      return col_id;
    } case LogicalOperatorType::LOGICAL_DELIM_GET: { // duplicate eliminated scan (output of distinct)
      auto &get = op->Cast<LogicalDelimGet>();
      LDEBUG("table_index: ", get.table_index, ", len(types): ", get.chunk_types.size());
      int col_id = get.chunk_types.size();
      get.chunk_types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));
      auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
          op->type, 1, col_id, 0);
      lop->AddChild(std::move(op));
      op = std::move(lop);
      return col_id; // TODO: adjust once I adjust distinct types
    } case LogicalOperatorType::LOGICAL_TOP_N: {
      return rowids[0];
    } case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: { // CTE_SCAN too
      return rowids[1];
    } case LogicalOperatorType::LOGICAL_CTE_REF: { // CTE_SCAN too
      // refrence a recursive CTE. table_index, cte_index, chunk_types, bound_columns
      // add lineage op to generate ids
      auto& col = op->Cast<LogicalCTERef>();
      idx_t col_id = col.chunk_types.size();
      LDEBUG("CETRef: ", col_id, " ", col.bound_columns[0]);
      col.chunk_types.push_back(LogicalType::ROW_TYPE);
      col.bound_columns.push_back("rowid");
      return col_id;
    } case LogicalOperatorType::LOGICAL_ORDER_BY: {
        // it passes through child types. except if projections is not empty, then we need to add it
        auto &order = op->Cast<LogicalOrder>();
        if (!order.projection_map.empty()) {
         // order.projections.push_back(); the rowid of child
        }
        return rowids[0];
    } case LogicalOperatorType::LOGICAL_CREATE_TABLE: {
      if (rowids.size() > 0)  return rowids[0];
      else return 0;
    } case LogicalOperatorType::LOGICAL_PROJECTION: {
      // projection, just make sure we propagate any annotation columns
      int col_id = rowids[0];
      op->expressions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, col_id));
      int new_col_id = op->expressions.size()-1;
      return new_col_id;
    } case LogicalOperatorType::LOGICAL_FILTER: {
      return HandleFilter(op, rowids);
  } case LogicalOperatorType::LOGICAL_UNION: {
      return HandleUnion(op, cur_op_id, query_id, rowids);
  } case LogicalOperatorType::LOGICAL_DELIM_JOIN:
    case LogicalOperatorType::LOGICAL_ASOF_JOIN:
    case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      return HandleJoin(op, rowids, query_id, cur_op_id);
  } case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:{
    // TODO: add logic to propagte both left and right. dont materialize lineage.
    auto &join = op->Cast<LogicalUnconditionalJoin>();
    idx_t left_col_id = rowids[0];
    idx_t right_col_id = rowids[1];
    LDEBUG("HandleCross: ",  left_col_id, right_col_id);
    const int source_count = 2;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id, query_id,
        op->type, source_count, left_col_id, right_col_id);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return left_col_id + right_col_id;
  } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
    return HandleAggregate(op, context, rowids, query_id, cur_op_id);
  } default: {}
  }
  return 0;
}

idx_t InjectLineageOperator(unique_ptr<LogicalOperator> &op,ClientContext &context, idx_t query_id) {
  if (!op) return 0;
  vector<idx_t> rowids;
  for (auto &child : op->children) {
    rowids.push_back( InjectLineageOperator(child, context,  query_id) );
  }

  idx_t op_id = LineageState::pointer_to_opid[(void*)op.get()];

  LDEBUG("InjectLineageOperator: type: ", EnumUtil::ToChars<LogicalOperatorType>(op->type),
        ", name: ", op->GetName(),  " op_id: ", op_id,  " rowids:[ ", MapToString(rowids), " ]" );
  return HandleOperator(op, context, query_id, rowids, op_id);
}

unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                    unique_ptr<LogicalOperator>& plan) {
  idx_t query_id = LineageState::qid_plans_roots.size();
  idx_t cur_op_id = 0;
  AnnotatePlan(plan, query_id, cur_op_id);
  idx_t final_rowid = InjectLineageOperator(plan, input.context, query_id);
  // inject lineage op at the root of the plan to extract any annotation columns
  // If root is create table, then add lineage operator below it
  LDEBUG("root: ", EnumUtil::ToChars<LogicalOperatorType>(plan->type),
         "Annotation Column: ",  final_rowid, " Operator Id: ", cur_op_id);
  idx_t root_id = LineageState::pointer_to_opid[(void*)plan.get()];
  auto root = make_uniq<LogicalLineageOperator>(plan->estimated_cardinality,
      root_id, query_id, plan->type, 1/*src_cnt*/, final_rowid, 0, true);
  LineageState::qid_plans_roots[query_id] = root->operator_id;
  /*if (plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
    auto child = std::move(plan->children[0]);
    root->AddChild(std::move(child));
    plan->children[0] = std::move(root);
  }*/
  
  root->AddChild(std::move(plan));
  plan = std::move(root);
  
  PostAnnotate(query_id, root_id);
  LineageState::pointer_to_opid.clear();

  return std::move(plan);
}

} // namespace duckdb
