#include "lineage/lineage_init.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

#include <iostream>
#include <string>

namespace duckdb {


// -------------------------------------------------------------
// Helpers
// -------------------------------------------------------------

static inline bool HasZones(const vector<vector<idx_t>> &zones, idx_t p_id) {
    return !zones.empty() && !zones[p_id].empty();
}

static inline idx_t ResolveOffset(const vector<vector<idx_t>> &zones,
                                   idx_t p_id, idx_t cur, idx_t fallback) {
    return HasZones(zones, p_id) ? zones[p_id][cur] : fallback;
}

// -------------------------------------------------------------
// PartitionedLineage
// -------------------------------------------------------------

idx_t PartitionedLineage::get_total_count() {
  idx_t total_count = 0;
  for (auto &partition : local_offsets) {
    for (auto& cnt : partition) {
      total_count += cnt;
    }
  }
  return total_count;
}

// -------------------------------------------------------------
// LIST-based lineage (aggregates)
// -------------------------------------------------------------

idx_t PartitionedLineage::fill_list_lineage(vector<vector<idx_t>>& glineage) {
  const idx_t total_groups = get_total_count();
  glineage.clear();
  glineage.resize(total_groups);

  idx_t p_id = 0;
  idx_t total_input = 0;

  for (auto &partition : left) {
    idx_t cur = 0;
    
    LDebug(StringUtil::Format("|partition|: {}", partition.size()));
    
    for (auto& entry :partition) {
      if (!entry.data || entry.count == 0) continue;

      idx_t p_offset = ResolveOffset(zones, p_id, cur, 0);
      
      auto list_entries = reinterpret_cast<const list_entry_t *>(entry.data);
     
      // The child data starts after all list_entry_t structures
      const idx_t entry_bytes = sizeof(list_entry_t) * entry.count;
      auto child_values = reinterpret_cast<const int64_t *>(entry.data + entry_bytes);
      ValidityMask mask;
      const bool has_validity = (!entry.is_valid && entry.validity);
      if (has_validity) {
          mask = ValidityMask(entry.validity, entry.count);
      }
      for (idx_t i = 0; i < entry.count; i++) {
          if (has_validity && !mask.RowIsValid(i)) continue;

          const auto &e = list_entries[i];
          auto &dst = glineage[i+p_offset];
          dst.resize(e.length);
          for (idx_t j = 0; j < e.length; j++) {
            dst[j] = child_values[e.offset + j];
          }
          total_input += e.length;
      }
      cur++;
    }

    p_id++;
  }
  return total_input;
}

// -------------------------------------------------------------
// 1D lineage (projection, filter, join sides)
// -------------------------------------------------------------


void PartitionedLineage::fill_local(vector<vector<IVector>>& store, vector<idx_t>& lineage1D) {
  idx_t global_offset = 0;
  idx_t p_id = 0;

  for (auto &partition : store) {
    idx_t cur = 0;
    for (auto& lin_n :partition) {
      const idx_t count = lin_n.count;
      const idx_t base_offset = ResolveOffset(zones, p_id, cur, global_offset);
      if (!lin_n.is_valid && !lin_n.data) {
        // TODO: need to mark it with null
        for (idx_t k = 0; k < count; ++k)  {
          lineage1D[base_offset + k] = 0;
        }
      } else {
        int64_t* col = reinterpret_cast<int64_t*>(lin_n.data);
        for (idx_t k = 0; k < count; ++k)  {
          lineage1D[base_offset + k] = col[k];
        }
      }
      cur++;
      global_offset += count;
    }
    p_id++;
  }
}

void PartitionedLineage::fill_lineage(bool is_left, vector<idx_t>& lineage1D, idx_t total_count) {
  lineage1D.clear();
  lineage1D.resize(total_count);

  fill_local(is_left ? left : right, lineage1D);

  if (LineageState::debug) {
    for (idx_t i=0; i < lineage1D.size(); ++i) {
      std::cout << i << " " << lineage1D[i] << std::endl;
    }
  }
}


// -------------------------------------------------------------
// Global Lineage Initialization
// -------------------------------------------------------------

idx_t InitGlobalLineageBuff(ClientContext& context, idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  string table = to_string(qid) + "_" + to_string(opid);

  // Initialize children first
  vector<idx_t> children_opid;
  for (auto &child_opid : lop_info->children) {
    idx_t cid = InitGlobalLineageBuff(context, qid, child_opid);
    children_opid.push_back(cid);
  }

  LDebug(StringUtil::Format("GetCachedLineage: {}, table: {}",
        EnumUtil::ToChars<LogicalOperatorType>(lop_info->type), table));

  vector<vector<idx_t>>& store = LineageState::lineage_global_store[table];

  switch (lop_info->type) {
   // -------------------------------------------------
   // Unary operators
   // -------------------------------------------------
   case LogicalOperatorType::LOGICAL_PROJECTION:
   case LogicalOperatorType::LOGICAL_TOP_N:
   case LogicalOperatorType::LOGICAL_ORDER_BY:
   case LogicalOperatorType::LOGICAL_FILTER: {

      if (!lop_info->materializes_lineage) return children_opid[0];
      store.clear();
      store.emplace_back();
      idx_t total_count = LineageState::partitioned_store_buf[table]->get_total_count();
      LineageState::partitioned_store_buf[table]->fill_lineage(true /*is_left*/, store[0], total_count);
      lop_info->n_output = store[0].size();
      LDebug(StringUtil::Format("|store[0]|: {} {} ", store[0].size(), total_count));
      return opid;
   }

   // -------------------------------------------------
   // Join
   // -------------------------------------------------
   case LogicalOperatorType::LOGICAL_DELIM_JOIN:
   case LogicalOperatorType::LOGICAL_ASOF_JOIN:
   case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
   case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      store.clear();
      store.resize(2);
      idx_t total_count = LineageState::partitioned_store_buf[table]->get_total_count();

      LineageState::partitioned_store_buf[table]->fill_lineage(false /*is_left*/, store[1], total_count);
      LineageState::partitioned_store_buf[table]->fill_lineage(true, store[0], total_count);

      lop_info->n_output = store[0].size();
      LDebug(StringUtil::Format("|store[0]|: {} |store[1]|: {}, total_count: {}",
            store[0].size(), store[1].size(), total_count));
      return opid;
   }
   // -------------------------------------------------
   // Aggregate (list lineage)
   // -------------------------------------------------
   case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      store.clear();
      idx_t total_count = LineageState::partitioned_store_buf[table]->fill_list_lineage(store);
      lop_info->n_output = store.size();
      lop_info->n_input = total_count;
      LDebug(StringUtil::Format("total_count: {} |store|: {}", total_count, store.size()));
      return opid;
   }
   default: {}
  }
  
  return DConstants::INVALID_INDEX;
}

}
