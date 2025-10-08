#include "lineage/logical_lineage_operator.hpp"

#include <iostream>

#include "lineage/lineage_init.hpp"
#include "lineage/physical_lineage_operator.hpp"
#include "fade/physical_caching_operator.hpp"

#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace duckdb {

LogicalLineageOperator::LogicalLineageOperator(idx_t estimated_cardinality,
    idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type,
    int source_count, idx_t left_rid, idx_t right_rid, bool is_root)  :
  operator_id(operator_id), query_id(query_id), 
  source_count(source_count), dependent_type(dependent_type), is_root(is_root),
  left_rid(left_rid), right_rid(right_rid),  mark_join(false) {
  this->estimated_cardinality = estimated_cardinality; 
  LineageState::qid_plans[query_id][operator_id]->has_lineage = true;
  if (LineageState::debug)
    std::cout << "Add LogicalLineageOperator with child type:" << EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
}

void LogicalLineageOperator::ResolveTypes()  {
  if (children.empty()) return;
  types = children[0]->types; // Copy types from child and log them
  if (LineageState::debug) {
    std::cout << "Resolve Types (child[0]): " << this->operator_id << " " <<  EnumUtil::ToChars<LogicalOperatorType>(dependent_type) << "\n";
    for (auto &type : types) { std::cout << type.ToString() << " ";}
     std::cout << "\n";
  }
  if (this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_GET) { 
    types.pop_back();
    types.push_back(LogicalType::ROW_TYPE);
    return;
  }
  if (this->dependent_type == LogicalOperatorType::LOGICAL_CHUNK_GET) { 
    types.push_back(LogicalType::ROW_TYPE);
    return;
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



PhysicalOperator& LogicalLineageOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
  // Get a plan for our child using the public API
  bool debug = false;
  string join_type = "";
  string table_name = to_string(query_id) + "_" + to_string(operator_id);
  LineageState::lineage_types[table_name] = dependent_type;

  if (this->dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN
       || this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
      auto& join = children[0]->Cast<LogicalJoin>();
      join_type = EnumUtil::ToChars<JoinType>(join.join_type);
  }
  
  auto &child = generator.CreatePlan(*children[0]);
  if (this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    // LOGICAL_DELIM_JOIN is not used if it doesn't have any 
    // this has distinct and join we need to modify
    auto& delim = child.Cast<PhysicalDelimJoin>(); // NOT TRUE ALL THE TIME, LOGICAL DELIM
                                                   // COULD SWITCH TO NORMAL JOIN. TODO: verify
    // this only wraps a regular join where one of its inputs is duplicate
    // eliminated using distinct
    if (LineageState::debug) {
      std::cout << "#################" << std::endl;
      std::cout << delim.ToString() << std::endl;
      std::cout << "#################" << std::endl;
      std::cout << delim.children.back().get().ToString() << std::endl;
    }

    // right or left delim join, both place the duplicate eliminated child as the last one
    // we maintain the guarantee that the last column is an annotation column
    auto last_col = delim.children.back().get().types.size()-1;
    auto &catalog = Catalog::GetSystemCatalog(context);
    auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
        context, DEFAULT_SCHEMA, "list");
    auto list_function = entry.functions.GetFunctionByArguments(context, {LogicalType::ROW_TYPE});
    auto rowid_colref = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::ROW_TYPE, last_col);
    vector<unique_ptr<Expression>> children;
    children.push_back(std::move(rowid_colref));
    unique_ptr<FunctionData> bind_info = list_function.bind(context, list_function, children);
    auto list_aggregate = make_uniq<BoundAggregateExpression>(list_function, std::move(children), nullptr,
        std::move(bind_info), AggregateType::NON_DISTINCT);
    auto& agg = delim.distinct.Cast<PhysicalHashAggregate>();

    agg.grouped_aggregate_data.aggregates.push_back(std::move(list_aggregate));
    agg.types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));

    vector<unsafe_vector<idx_t>> grouping_functions;
    delim.distinct.grouped_aggregate_data.InitializeGroupby(std::move(agg.grouped_aggregate_data.groups),
                                             std::move(agg.grouped_aggregate_data.aggregates),
                                             std::move(grouping_functions));
    delim.distinct.non_distinct_filter.push_back(0);
	  delim.distinct.distinct_collection_info = DistinctAggregateCollectionInfo::Create(delim.distinct.grouped_aggregate_data.aggregates);
    delim.distinct.groupings.clear();
    for (idx_t i = 0; i < delim.distinct.grouping_sets.size(); i++) {
      delim.distinct.groupings.emplace_back(delim.distinct.grouping_sets[i],
          delim.distinct.grouped_aggregate_data, delim.distinct.distinct_collection_info);
    }
    
    if (LineageState::debug) { std::cout << delim.distinct.ToString() << std::endl; }
  }
  // if we need payload
  if (LineageState::cache && this->dependent_type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    auto agg_types = child.children[0].get().GetTypes();
    // Replace agg child with caching op
    auto agg_child = child.children[0];
    child.children[0] = generator.Make<PhysicalCachingOperator>(agg_types, agg_child,
                                      operator_id, query_id, child);
  }
  
  return generator.Make<PhysicalLineageOperator>(types, child, operator_id, query_id, dependent_type,
      source_count, left_rid, right_rid, is_root, join_type);
}
}
