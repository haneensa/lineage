// physical_caching_operator.hpp
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include <iostream>

namespace duckdb {

// Our actual physical operator
class PhysicalCachingOperator : public PhysicalOperator {
public:
    PhysicalCachingOperator(vector<LogicalType> types, PhysicalOperator&child,
        idx_t operator_id, idx_t query_id);

    OperatorResultType Execute(ExecutionContext &context,
                             DataChunk &input, 
                             DataChunk &chunk,
                             GlobalOperatorState &gstate,
                             OperatorState &state) const override;

    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;


    bool ParallelOperator() const override {
      return true;
    }

    string GetName() const override {
        return "CACHING";
    }
public:
    idx_t operator_id;
    idx_t query_id;
};


} // namespace duckdb
