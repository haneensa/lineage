#include "lineage/lineage_init.hpp"

#include <iostream>

#include "fade_extension.hpp"
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
      ) != true) return false;
  
  for (auto &child : plan->children) {
    if (IsSPJUA(child) == false) return false;
  }

  return true;
}

// check also lineage size matches n_input / n_output
void get_cached_lineage(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
   get_cached_lineage(qid, child);
  }

  string table = to_string(qid) + "_" + to_string(opid);
   if (LineageState::debug)
    std::cout << "pre get cached lineage: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
      << " " << table << " " << lop_info->lineage1D.size() << " " << lop_info->lineage2D.size() << " " << lop_info->n_input
      << std::endl;
  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      break;
   } case LogicalOperatorType::LOGICAL_FILTER: {
      vector<std::pair<Vector, int>>& chunked_lineage = LineageState::lineage_store[table];
      for (auto& lin_n :chunked_lineage) {
        int64_t* col = reinterpret_cast<int64_t*>(lin_n.first.GetData());
        idx_t count = lin_n.second;
        for (idx_t i = 0; i < count; i++) {
          lop_info->lineage1D.push_back(col[i]);
        }
      }
     break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      for (idx_t i=0; i < 2; ++i) {
        if (i == 1) table += "_right";
        vector<std::pair<Vector, int>>& chunked_lineage = LineageState::lineage_store[table];
        lop_info->lineage2D.emplace_back();
        for (auto& lin_n :chunked_lineage) {
          int64_t* col = reinterpret_cast<int64_t*>(lin_n.first.GetData());
          idx_t count = lin_n.second;
          for (idx_t i = 0; i < count; i++) {
            lop_info->lineage2D.back().push_back(col[i]);
          }
        }
      }
     break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      // for each chunk, iterate over the values
      vector<std::pair<Vector, int>>& chunked_lineage = LineageState::lineage_store[table];
      lop_info->lineage1D.resize(lop_info->n_input); // n_input initialized in the cache op
      for (auto& lin_n :chunked_lineage) {
        idx_t count = lin_n.second;
        auto &list_vec = ListVector::GetEntry(lin_n.first); // child vector
        auto child_data = FlatVector::GetData<int64_t>(list_vec); // underlying int data
        auto list_data = FlatVector::GetData<list_entry_t>(lin_n.first); // offsets and lengths
        for (idx_t i = 0; i < count; i++) {
            auto entry = list_data[i];
            lop_info->lineage2D.emplace_back();
            for (idx_t j = 0; j < entry.length; j++) {
              lop_info->lineage2D.back().push_back(child_data[entry.offset + j]);
              lop_info->lineage1D[child_data[entry.offset+j]] = i;
            }
        }
      }

     break;
   } default: {}}
  // TODO: handle order by
   if (LineageState::debug)
    std::cout << "post get cached lineage: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
      << " " << table << " " << lop_info->lineage1D.size() << " " << lop_info->lineage2D.size()
      << std::endl;

}

void populate_and_verify_n_input_output(idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  for (auto &child : lop_info->children) {
   populate_and_verify_n_input_output(qid, child);
  }
  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      idx_t n_input = FadeState::table_count[lop_info->table_name];
      lop_info->n_input = n_input;
      lop_info->n_output = n_input;
      break;
   } case LogicalOperatorType::LOGICAL_FILTER: {
      lop_info->n_input = LineageState::qid_plans[qid][ lop_info->children[0] ]->n_output;
      break;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      lop_info->n_input = LineageState::qid_plans[qid][ lop_info->children[0] ]->n_output;
      break;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      // set by the cache operator. just need to verify
      idx_t child_n_output = LineageState::qid_plans[qid][ lop_info->children[0] ]->n_output;
      assert(lop_info->n_input == child_n_output);
      break;
		} default: {}
   }

  if (LineageState::debug)
    std::cout << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type) << " n_input: "
      << lop_info->n_input << ", n_output: " << lop_info->n_output << std::endl;
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

  if (LineageState::debug)  std::cout << "[DEBUG] Join: " << EnumUtil::ToChars<JoinType>(join.join_type) <<
      "-> " << left_col_id + right_col_id << " " << left_col_id << " " << right_col_id
      << " " << rowids[0] << " " << rowids[1] << " " << join.left_projection_map.size() << " "
      << join.right_projection_map.size() << std::endl;

  int source_count = 2;
  auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id++, query_id,
      op->type, source_count, left_col_id, right_col_id);
  lop->AddChild(std::move(op));
  op = std::move(lop);
  return left_col_id + right_col_id;
}

bool filter_between_agg_and_base(unique_ptr<LogicalOperator>& op) {
  if (op->type == LogicalOperatorType::LOGICAL_GET) {
    auto &get = op->Cast<LogicalGet>();
    return !get.table_filters.filters.empty(); // filter pushdown
  } else if (op->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
    return false;
  } else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
    return true;
  } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
    return filter_between_agg_and_base(op->children[0]);
  }
  return false;
}


idx_t InjectLineageOperator(unique_ptr<LogicalOperator> &op,ClientContext &context, idx_t query_id, idx_t& cur_op_id) {
  if (!op) return 0;
  vector<idx_t> rowids = {};

  for (auto &child : op->children) {
    rowids.push_back( InjectLineageOperator(child, context,  query_id, cur_op_id) );
  }

  if (LineageState::debug) {
    std::cout << "Inject: " << op->GetName() << " " << cur_op_id;
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
    if (LineageState::debug)  std::cout << "[DEBUG] LogicalGet: " << col_id  << std::endl;
    return col_id;
  }  else if (op->type == LogicalOperatorType::LOGICAL_CHUNK_GET) { // CTE_SCAN too
    // add lineage op to generate ids
    auto& col = op->Cast<LogicalColumnDataGet>();
    idx_t col_id = col.chunk_types.size();
    if (LineageState::debug) std::cout << "[DEBUG] ChunkGet: " << col_id << std::endl;
    auto lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id++, query_id,
        op->type, 1/*src cnt*/, col_id, 0);
    lop->AddChild(std::move(op));
    op = std::move(lop);
    return col_id;
  }  else if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) { // CTE_SCAN too
      if (LineageState::debug)  std::cout << "[DEBUG] MateralizedCTE: " << rowids[0] << " " << rowids[1] << std::endl;
      return rowids[1];
  }  else if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) { // CTE_SCAN too
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
    if (!filter.projection_map.empty()) {
        filter.projection_map.push_back(col_id); 
        new_col_id = filter.projection_map.size()-1; 
    }
    if (LineageState::debug)  std::cout << "[DEBUG] Filter " << col_id << " " << new_col_id << std::endl;
    return new_col_id;
  } else if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
    // it passes through child types. except if projections is not empty, then we need to add it
    auto &order = op->Cast<LogicalOrder>();
    if (LineageState::debug) std::cout << "[DEBUG] OrderBy: " << rowids[0] << std::endl;
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
    if (LineageState::debug) std::cout << "[DEBUG] Projection: " << col_id << " " << new_col_id  << "\n";
    return new_col_id;
  } else if (op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
  } else if (op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
  } else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
    // Propagate annotations from the left and right sides.
    // Add PhysicaLineage to extraxt the last two columns
    // and replace it with a single annotations column
    return ProcessJoin(op, rowids, query_id, cur_op_id);
  } else if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
      // check if there is a filter between this and last lineageop or leaf node
      bool is_filter_child = filter_between_agg_and_base(op->children[0]);
      if (is_filter_child) {
        auto filter_lop = make_uniq<LogicalLineageOperator>(op->estimated_cardinality, cur_op_id++,
            query_id, LogicalOperatorType::LOGICAL_FILTER, 1, rowids[0], 0, 0);
        auto child_child = std::move(op->children[0]);
        filter_lop->AddChild(std::move(child_child));
        op->children[0] = std::move(filter_lop);
      }
      auto &aggr = op->Cast<LogicalAggregate>();
     // if (!aggr.groups.empty()) {
          if (LineageState::debug) std::cout << "[DEBUG] Modifying Aggregate operator\n";
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
          
          auto dummy = make_uniq<LogicalLineageOperator>(aggr.estimated_cardinality, cur_op_id++, query_id,
              op->type, 1, new_col_id, 0);
          dummy->AddChild(std::move(op));

          op = std::move(dummy);
          
          return new_col_id;
    //  } // if simple agg, add operator below to remove annotations, and operator above to generate annotations
  }
  return 0;
}

idx_t BuildLineageInfoTree(unordered_map<idx_t, unique_ptr<LineageInfoNode>>&lop_plan,
                           unique_ptr<LogicalOperator>&plan, idx_t& cur_op_id) {
  if (plan->children.size() == 0) {
    idx_t opid = cur_op_id++;
    lop_plan[opid] =  make_uniq<LineageInfoNode>(opid, plan->type);
    if (plan->type == LogicalOperatorType::LOGICAL_GET) {
      auto &get = plan->Cast<LogicalGet>();
      if (get.GetTable()) {
        string table_name = get.GetTable()->name;
        lop_plan[opid]->table_name = table_name;
        lop_plan[opid]->columns = get.names;
      }
    }
    return opid;
  }
  
  // if this is an extension for lineage then get left and right childrens
  idx_t child = BuildLineageInfoTree(lop_plan, plan->children[0], cur_op_id);

  if (plan->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
    idx_t opid = plan->Cast<LogicalLineageOperator>().operator_id;
    auto lop = make_uniq<LineageInfoNode>(opid,
        plan->Cast<LogicalLineageOperator>().dependent_type);
    lop->children.push_back(child);
    if (plan->children[0]->children.size() > 1) {
      idx_t rhs_child = BuildLineageInfoTree(lop_plan, plan->children[0]->children[1], cur_op_id);
      lop->children.push_back(rhs_child);
    }
    
    if (lop->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    }
    lop_plan[opid] = std::move(lop);
    return opid;
  }

  return child;
}
  
unique_ptr<LogicalOperator> AddLineage(OptimizerExtensionInput &input,
                                    unique_ptr<LogicalOperator>& plan) {
  idx_t query_id = LineageState::query_id++; 
  idx_t cur_op_id = 0;
  idx_t final_rowid = InjectLineageOperator(plan, input.context, query_id, cur_op_id);
  // inject lineage op at the root of the plan to extract any annotation columns
  // If root is create table, then add lineage operator below it
  if (LineageState::debug)
    std::cout << "final rowid: " << final_rowid << " " << cur_op_id << std::endl;
  auto root = make_uniq<LogicalLineageOperator>(plan->estimated_cardinality,
      cur_op_id++, query_id, plan->children[0]->type, 1/*src_cnt*/, final_rowid, 0, true);
  LineageState::qid_plans_roots[query_id] = root->operator_id;
  if (plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
    auto child = std::move(plan->children[0]);
    root->AddChild(std::move(child));
    plan->children[0] = std::move(root);
  } else {
    root->AddChild(std::move(plan));
    plan = std::move(root);
  }
  BuildLineageInfoTree(LineageState::qid_plans[query_id], plan, cur_op_id);

  return std::move(plan);
}

} // namespace duckdb
