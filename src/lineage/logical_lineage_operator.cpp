#include "lineage/logical_lineage_operator.hpp"

#include "lineage/lineage_init.hpp"
#include "lineage/physical_lineage_operator.hpp"

#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

LogicalLineageOperator::LogicalLineageOperator(idx_t estimated_cardinality,
    idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type,
    int source_count, idx_t left_rid, idx_t right_rid, bool is_root)  :
  operator_id(operator_id), query_id(query_id), 
  source_count(source_count), dependent_type(dependent_type), is_root(is_root),
  left_rid(left_rid), right_rid(right_rid),  mark_join(false) {
  this->estimated_cardinality = estimated_cardinality; 
  LineageState::qid_plans[query_id][operator_id]->materializes_lineage = true;
  string table_name = to_string(query_id) + "_" + to_string(operator_id);
  LineageState::lineage_types[table_name] = dependent_type;
  LDebug( StringUtil::Format("Add LogicalLineageOperator with child type: {}",
         EnumUtil::ToChars<LogicalOperatorType>(dependent_type)) );
}

void LogicalLineageOperator::ApplyMarkJoinRewrite() {
  // if mark join, then need to move the end of the left child to the last column
  types.erase(types.begin() + left_rid);
  types.push_back(LogicalType::ROW_TYPE);
  LDebug( StringUtil::Format("Mark Join: {} {} {}", left_rid, TypesToString(types)) );
}

void LogicalLineageOperator::ApplyJoinRewrite() {
  auto& join = children[0]->Cast<LogicalJoin>();
  LDebug( StringUtil::Format("Child[0] types with left_rid: {} {}",
        left_rid,TypesToString(children[0]->children[0]->types)) );
  LDebug( StringUtil::Format("Child[1] types with right_rid: {} {}",
        left_rid,TypesToString(children[0]->children[1]->types)) );
  
  if (this->IsSemiOrAnti(join)) {
    return;
  }
  types.erase(types.begin() + left_rid);
}

void LogicalLineageOperator::HandleOperatorSpecificAdjustments() {
    switch (dependent_type) {
    case LogicalOperatorType::LOGICAL_DELIM_GET:
        types.pop_back();
        types.push_back(LogicalType::ROW_TYPE);
        return;
    case LogicalOperatorType::LOGICAL_CHUNK_GET:
        types.push_back(LogicalType::ROW_TYPE);
        return;
    default:
        break;
    }

    if (mark_join) {
        ApplyMarkJoinRewrite();
        return;
    }

    if (IsJoin()) {
        ApplyJoinRewrite();
        return;
    }

    // default case
    types.pop_back();
    if (!is_root) {
        types.push_back(LogicalType::ROW_TYPE);
    }
}

void LogicalLineageOperator::ResolveTypes()  {
  if (children.empty()) return;

  // Copy Child Types
  types = children[0]->types;
  LDebug( StringUtil::Format("ResolveTypes: {} {} {}",
      operator_id, EnumUtil::ToChars(dependent_type), TypesToString(types)) );
  
  HandleOperatorSpecificAdjustments();
}

vector<ColumnBinding> LogicalLineageOperator::GetColumnBindings() {
  if (children.empty()) return {};
  
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

    string join_binding_str = "";
    for (auto &binding : join.children[0]->GetColumnBindings()) { join_binding_str += binding.ToString() + " "; }
    LDebug(StringUtil::Format("mark join binding: {} {} {} ( join left: {} )",
          left_rid, child_bindings.size(), types.size(), join_binding_str) );

    auto left_most = child_bindings[left_rid];
    child_bindings.erase(child_bindings.begin() + left_rid);
    child_bindings.push_back(left_most);
    // get bindings of child
    string child_binding_str = "";
    for (auto &binding : child_bindings) { child_binding_str += binding.ToString() + " "; }
    LDebug(StringUtil::Format("child binding: {}", child_binding_str) );
    return child_bindings;
  }
  
  if (this->IsJoin()) {
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
  return child_bindings;
}



void LogicalLineageOperator::RewriteDelimJoin(
    ClientContext &context, PhysicalOperator &child) {
    // LOGICAL_DELIM_JOIN is not used if it doesn't have any 
    // this has distinct and join we need to modify
    auto& delim = child.Cast<PhysicalDelimJoin>(); // NOT TRUE ALL THE TIME, LOGICAL DELIM
                                                   // COULD SWITCH TO NORMAL JOIN. TODO: verify
    // this only wraps a regular join where one of its inputs is duplicate
    // eliminated using distinct
    LDebug("######## delim #########\n" + delim.ToString());
    LDebug("######## delim distinct pre #########\n" + delim.children.back().get().ToString());

    // right or left delim join, both place the duplicate eliminated child as the last one
    // we maintain the guarantee that the last column is an annotation column
    auto last_col = delim.children.back().get().types.size()-1;
    auto &catalog = Catalog::GetSystemCatalog(context);
    auto rowid_colref = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::ROW_TYPE, last_col);
    vector<unique_ptr<Expression>> children;
    children.push_back(std::move(rowid_colref));
    auto& agg = delim.distinct.Cast<PhysicalHashAggregate>();
    string fname = "list";
    agg.types.push_back(LogicalType::LIST(LogicalType::ROW_TYPE));
    auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, fname);
    auto list_function = entry.functions.GetFunctionByArguments(context, {LogicalType::ROW_TYPE});
    unique_ptr<FunctionData> bind_info = list_function.bind(context, list_function, children);
    auto list_aggregate = make_uniq<BoundAggregateExpression>(list_function, std::move(children), nullptr,
        std::move(bind_info), AggregateType::NON_DISTINCT);
    agg.grouped_aggregate_data.aggregates.push_back(std::move(list_aggregate));

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
    
    LDebug("######## delim distinct post #########\n" + delim.children.back().get().ToString());
}

string LogicalLineageOperator::ExtractJoinType() {
  if (this->IsJoin()) {
      auto& join = children[0]->Cast<LogicalJoin>();
      return EnumUtil::ToChars<JoinType>(join.join_type);
  }
  return "";
}

PhysicalOperator& LogicalLineageOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
  string join_type = ExtractJoinType();
  
  // Get a plan for our child using the public API
  auto &child = generator.CreatePlan(*children[0]);

  if (this->dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
    RewriteDelimJoin(context, child);
  }
  
  return generator.Make<PhysicalLineageOperator>(types, child, operator_id, query_id, dependent_type,
      source_count, left_rid, right_rid, is_root, join_type);
}
}
