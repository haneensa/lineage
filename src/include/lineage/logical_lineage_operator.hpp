#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

// Our logical operator that extends LogicalExtensionOperator
class LogicalLineageOperator : public LogicalExtensionOperator {
public:
    explicit LogicalLineageOperator(idx_t estimated_cardinality,
          idx_t operator_id, idx_t query_id, LogicalOperatorType dependent_type,
          int source_count, idx_t left_rid, idx_t right_rid, bool is_root=false);
    string GetName() const override {
        return "LINEAGE_OPERATOR";
    }
    
    PhysicalOperator& CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;

    bool IsJoin() const {
        return dependent_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
               dependent_type == LogicalOperatorType::LOGICAL_DELIM_JOIN;
    }

    bool IsSemiOrAnti(const LogicalJoin &join) const {
    return join.join_type == JoinType::SEMI ||
           join.join_type == JoinType::ANTI ||
           join.join_type == JoinType::RIGHT_SEMI ||
           join.join_type == JoinType::RIGHT_ANTI;
    }

    void HandleOperatorSpecificAdjustments();
    void ApplyJoinRewrite();
    void ApplyMarkJoinRewrite();
    void RewriteDelimJoin(ClientContext &context, PhysicalOperator &child);
    string ExtractJoinType();
    
protected:
    void ResolveTypes() override;
    vector<ColumnBinding> GetColumnBindings() override;


public:
    idx_t operator_id;
    idx_t query_id;
    idx_t left_rid;
    idx_t right_rid;
    int source_count;
    LogicalOperatorType dependent_type;
    bool is_root;
    bool mark_join;
    string join_type = "";
};

}
