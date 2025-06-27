#include "lineage/logical_lineage_operator.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"
#include "lineage/physical_lineage_operator.hpp"
#include "lineage/physical_caching_operator.hpp"

#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

LogicalLineageOperator::LogicalLineageOperator(idx_t estimated_cardinality,
    idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type,
    int source_count, idx_t left_rid, idx_t right_rid, bool is_root)  :
  operator_id(operator_id), query_id(query_id), 
  source_count(source_count), dependent_type(dependent_type), is_root(is_root),
  left_rid(left_rid), right_rid(right_rid),  mark_join(false) {
  this->estimated_cardinality = estimated_cardinality; 
  if (LineageState::debug)
    std::cout << "LogicalLineageOperator with child type:" << EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
}

void LogicalLineageOperator::ResolveTypes()  {
  if (children.empty()) return;
  types = children[0]->types; // Copy types from child and log them
  if (this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_GET) { 
    types.pop_back();
    types.push_back(LogicalType::ROW_TYPE);
    return;
  }
  if (this->dependent_type == LogicalOperatorType::LOGICAL_CHUNK_GET) { 
    types.push_back(LogicalType::ROW_TYPE);
    return;
  }
  if (LineageState::debug) {
    std::cout << "Resolve Types (child[0]): " << this->operator_id << " " <<  EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
    for (auto &type : types) { std::cout << type.ToString() << " ";}
     std::cout << "\n";
  }
  if (mark_join) {
    // if mark join, then need to move the end of the left child to the last column
    types.erase(types.begin() + left_rid);
    types.push_back(LogicalType::ROW_TYPE);
    if (LineageState::debug) {
      std::cout << "Mark join " << left_rid << std::endl;
      for (auto &type : types) { std::cout << type.ToString() << " "; }
      std::cout << "\n";
    }
    return;
  }
  if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN
     || this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    auto& join = children[0]->Cast<LogicalJoin>();
    if (LineageState::debug) {
      std::cout << "Child[0] types with left_rid: " << left_rid << std::endl;
      for (auto &type : children[0]->children[0]->types) { std::cout << type.ToString() << " "; }
      std::cout << "\n";
      
      std::cout << "Child[1] types with right_rid: " << right_rid << std::endl;
      for (auto &type : children[0]->children[1]->types) { std::cout << type.ToString() << " "; }
      std::cout << "\n";
    }
    if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI
     || join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
      return;
    }
    types.erase(types.begin() + left_rid);
  }
  types.pop_back();
  if (!is_root) types.push_back(LogicalType::ROW_TYPE);
}

vector<ColumnBinding> LogicalLineageOperator::GetColumnBindings() {
  if (children.empty()) return {};
//  std::cout << "[ Child type: " << EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
  auto child_bindings = children[0]->GetColumnBindings();
  if (this->dependent_type == LogicalOperatorType::LOGICAL_CHUNK_GET) { 
    if (child_bindings.empty()) return child_bindings;
    idx_t table_index = child_bindings.back().table_index;
    child_bindings.emplace_back(table_index, child_bindings.size());
    return child_bindings;
  }

  if (LineageState::debug) {
    std::cout << this->operator_id << "[DEBUG] Child column bindings " <<  EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
    for (auto &binding : child_bindings) { std::cout << binding.ToString() << " ";}
    std::cout << "\n";
  }
  if (this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_GET) { 
    return child_bindings; 
  }

  if (mark_join) {
    auto& join = children[0]->children[0]->Cast<LogicalJoin>();
    if (LineageState::debug) {
      std::cout << " mark join binding: " << left_rid << " " << child_bindings.size() << " " << types.size() << std::endl;
      std::cout << "( join left: " << std::endl;
      for (auto &binding : join.children[0]->GetColumnBindings()) { std::cout << binding.ToString() << " "; }
      std::cout << "\n ) " << left_rid << " " << child_bindings.size() <<  "\n";
      //for (auto &binding : child_bindings) { std::cout << binding.ToString() << " ";}
      // std::cout << "\n";
    }
    auto left_most = child_bindings[left_rid];
    child_bindings.erase(child_bindings.begin() + left_rid);
    child_bindings.push_back(left_most);
    // get bindings of child
    if (LineageState::debug) {
      for (auto &binding : child_bindings) { std::cout << " ---> " << binding.ToString() << " ";}
       std::cout << "\n";
    }
    return child_bindings;
  }

  if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN
       || this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto& join = children[0]->Cast<LogicalJoin>();
      //  std::cout << "join left: " << std::endl;
      //  for (auto &binding : join.children[0]->GetColumnBindings()) { std::cout << binding.ToString() << " "; }
      //  std::cout << "\n";
      //  std::cout << "join right: " << std::endl;
      //  for (auto &binding : join.children[1]->GetColumnBindings()) { std::cout << binding.ToString() << " "; }
      //  std::cout << "\n";
      if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI
       || join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
        return child_bindings;
      }
      child_bindings.erase(child_bindings.begin() + left_rid);
    //  std::cout << "-> join binding: " << left_rid << " " << child_bindings.size() << " " << types.size() << std::endl;
    //  for (auto &binding : child_bindings) { std::cout << binding.ToString() << " ";}
    //  std::cout << "\n<- ";
  } 
  //std::cout << "done ]" << std::endl;
  return child_bindings;
}

// TODO: support distinct yet.
void get_agg_info(unique_ptr<AggInfo>& info, vector<unique_ptr<Expression>>& aggs, vector<LogicalType>& payload_types) {
  if (LineageState::debug) std::cout << "get_agg_info: " << info->n_groups_attr << " " << aggs.size() << std::endl;
  int include_count = false;
  idx_t count_idx = aggs.size();
  // -1 excluding the lineage capture function
  for (idx_t i=0;  i < aggs.size()-1; ++i) {
    auto &agg_expr = aggs[i]->Cast<BoundAggregateExpression>();
    string name = agg_expr.function.name;
    if (LineageState::debug) std::cout << i << " agg: " << name << std::endl;
		if (include_count == false && (name == "count" || name == "count_star")) {
			include_count = true;
      count_idx = i;
			continue;
		} else if (name == "avg") {
			include_count = true;
		}
		
    if (name == "sum_no_overflow") name = "sum";
		
    if (name == "sum" || name == "avg" || name == "stddev") {
      D_ASSERT(agg_expr.children.size() > 1);
      D_ASSERT(agg_expr.children[0]->type == ExpressionType::BOUND_REF);
      auto &bound_ref_expr = agg_expr.children[0]->Cast<BoundReferenceExpression>();
			int col_idx = bound_ref_expr.index; 
      LogicalType ret_typ = LogicalType::FLOAT;
      LogicalType default_typ = LogicalType::FLOAT;
      if (payload_types[col_idx] == LogicalType::INTEGER ||
          payload_types[col_idx] == LogicalType::BIGINT) {
           default_typ = LogicalType::INTEGER;
           ret_typ = LogicalType::INTEGER;
      }
      info->payload_data.push_back({col_idx, default_typ});

      string sum_func_key = "sum_" + to_string(i);
      vector<string> sub_aggs_list = {sum_func_key};
      info->sub_aggs[sum_func_key] = make_uniq<SubAggsContext>("sum", default_typ, col_idx, i);

      if (name == "stddev") {
        string sum_2_func_key = "sum_2_" + to_string(i);
        info->sub_aggs[sum_2_func_key] = make_uniq<SubAggsContext>("sum_2", LogicalType::FLOAT, col_idx, i);
        sub_aggs_list.push_back(sum_2_func_key);
        ret_typ = LogicalType::FLOAT;
      }
      if (name == "avg" || name == "stddev") {
        sub_aggs_list.push_back("count");
        include_count = true;
        ret_typ = LogicalType::FLOAT;
      }
      info->aggs[i] = make_uniq<AggFuncContext>(name, ret_typ, std::move(sub_aggs_list));
    }
  }

  if (include_count) {
    info->sub_aggs["count"] = make_uniq<SubAggsContext>("count", LogicalType::INTEGER, 0, 0);
    vector<string> sub_aggs_list = {"count"};
    info->aggs[count_idx] = make_uniq<AggFuncContext>("count", LogicalType::INTEGER,
                                              std::move(sub_aggs_list));
  }
  
  // TODO: check if this has another child aggregate. set: has_agg_child and child_agg_id
}


PhysicalOperator& LogicalLineageOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
  // Get a plan for our child using the public API
  bool debug = false;
  string join_type = "";
  if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      auto& join = children[0]->Cast<LogicalJoin>();
      join_type = EnumUtil::ToChars<JoinType>(join.join_type);
  }
  
  if (this->dependent_type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    auto agg_info = make_uniq<AggInfo>();
    agg_info->n_groups_attr = children[0]->Cast<LogicalAggregate>().groups.size();
    LineageState::qid_plans[query_id][operator_id]->agg_info = std::move(agg_info);
  }

  auto &child = generator.CreatePlan(*children[0]);
  if (LineageState::debug) {
    std::cout << "[DEBUG] LogicalLineageOperator::CreatePlan. " << std::endl;
    std::cout << child.ToString() << std::endl;
  }

  if (this->dependent_type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    auto &agg_info = LineageState::qid_plans[query_id][operator_id]->agg_info;
    auto agg_types = child.children[0].get().GetTypes();
	  if (child.type == PhysicalOperatorType::HASH_GROUP_BY) {
			PhysicalHashAggregate * gb = dynamic_cast<PhysicalHashAggregate *>(&child);
			auto &aggregates = gb->grouped_aggregate_data.aggregates;
      get_agg_info(agg_info, aggregates, agg_types);
    } else if (child.type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
			PhysicalPerfectHashAggregate * gb = dynamic_cast<PhysicalPerfectHashAggregate *>(&child);
      auto &aggregates = gb->aggregates;
      get_agg_info(agg_info, aggregates, agg_types);
    } else {
      PhysicalUngroupedAggregate * gb = dynamic_cast<PhysicalUngroupedAggregate *>(&child);
			auto &aggregates = gb->aggregates;
      get_agg_info(agg_info, aggregates, agg_types);
    }
    // Replace agg child with caching op
    auto agg_child = child.children[0];
    child.children[0] = generator.Make<PhysicalCachingOperator>(agg_types, agg_child, operator_id, query_id);

  }

  return generator.Make<PhysicalLineageOperator>(types, child, operator_id, query_id, dependent_type,
      source_count, left_rid, right_rid, is_root, join_type);
}
}
