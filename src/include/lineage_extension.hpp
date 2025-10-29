#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

AggregateFunction GetLineageUDAAllEvenFunction();

class LineageExtension : public Extension {
public:
   void Load(DuckDB &db) override;
   std::string Name() override;
   std::string Version() const override { return "v0.0.0"; }
};

} // namespace duckdb
