#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

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
};

}
