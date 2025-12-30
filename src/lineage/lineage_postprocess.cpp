#include "lineage/lineage_init.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

#include <iostream>
#include <string>

namespace duckdb {

// TODO: where forward lineage? we should store that.
idx_t PartitionedLineage::fill_list_lineage(vector<vector<idx_t>>& glineage) {
  idx_t total_groups = this->get_total_count();
  glineage.resize(total_groups);
  idx_t p_id = 0;
  idx_t total_cnt = 0;
  idx_t total_input = 0;
  for (auto &partition : left) {
    if (LineageState::debug)  std::cout << "chunked_lineage: " << partition.size() << std::endl;
    idx_t cur = 0;
    idx_t offset = 0;
    for (auto& entry :partition) {
      if (!entry.data || entry.count == 0) continue;
      idx_t p_offset = 0;
      if (!zones.empty()) {
        p_offset = zones[p_id][cur];
        // std::cout << "p_id: " << p_id << " cur: " << cur << " p : " << p_offset << " " << offset << std::endl;
      }
      auto list_entries = reinterpret_cast<const list_entry_t *>(entry.data);
      // The child data starts after all list_entry_t structures
      const idx_t entry_bytes = sizeof(list_entry_t) * entry.count;
      auto child_values = reinterpret_cast<const int64_t *>(entry.data + entry_bytes);
      ValidityMask mask;
      if (!entry.is_valid && entry.validity) {
          mask = ValidityMask(entry.validity, entry.count);
      }
      for (idx_t i = 0; i < entry.count; i++) {
          if (!entry.is_valid && entry.validity && !mask.RowIsValid(i)) continue;
          const auto &e = list_entries[i];
          glineage[i+p_offset].resize(e.length);
          for (idx_t j = 0; j < e.length; j++) {
            glineage[i+p_offset][j] = child_values[e.offset + j];
          }
          total_input += e.length;
      }
      offset += entry.count;
      cur++;
    }
    total_cnt += offset;
    p_id++;
  }
  return total_input;
}

void PartitionedLineage::fill_local(vector<vector<IVector>>& store, vector<idx_t>& lineage1D) {
  idx_t p_id = 0;
  idx_t offset = 0;
  for (auto &partition : store) {
    if (LineageState::debug)
      std::cout << "chunked_lineage: " << partition.size() << std::endl;
    idx_t cur = 0;
    for (auto& lin_n :partition) {
      idx_t count = lin_n.count;
      if (!lin_n.is_valid && !lin_n.data) {
        // TODO: need to mark it with null
        if (zones.empty() || zones[p_id].empty()) {
          for (idx_t k = 0; k < count; ++k)  lineage1D[k+offset] = 0;
        } else {
          idx_t partition_offset = zones[p_id][cur];
          for (idx_t k = 0; k < count; ++k)  {
            lineage1D[k+partition_offset] = 0;
          }
        }
      } else {
        int64_t* col = reinterpret_cast<int64_t*>(lin_n.data);
        if (zones.empty() || zones[p_id].empty()) {
          for (idx_t k = 0; k < count; ++k)  {
            lineage1D[k+offset] = col[k];
          }
        } else {
          idx_t partition_offset = zones[p_id][cur];
        //  std::cout << p_id << " " << cur << "partition_offset: " << partition_offset << " " << col[0] << " " << std::endl;
          for (idx_t k = 0; k < count; ++k)  {
            lineage1D[k+partition_offset] = col[k];
          }
        }
      }
      cur++;
      offset += count;
    }
    p_id++;
  }
}

idx_t PartitionedLineage::get_total_count() {
  idx_t total_count = 0;
  for (auto &partition : local_offsets) {
    for (auto& cnt : partition) {
      total_count += cnt;
    }
  }
  return total_count;
}
void PartitionedLineage::fill_lineage(bool is_left, vector<idx_t>& lineage1D, idx_t total_count) {
  lineage1D.resize(total_count);

  if (is_left)  this->fill_local(left, lineage1D);
  else this->fill_local(right, lineage1D);
  if (LineageState::debug) {
    for (idx_t i=0; i < lineage1D.size(); ++i) {
      std::cout << i << " " << lineage1D[i] << std::endl;
    }
  }
}

idx_t InitGlobalLineageBuff(ClientContext& context, idx_t qid, idx_t opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  string table = to_string(qid) + "_" + to_string(opid);
  vector<idx_t> children_opid;
  for (auto &child_opid : lop_info->children) {
    idx_t cid = InitGlobalLineageBuff(context, qid, child_opid);
    children_opid.push_back(cid);
  }

 if (LineageState::debug)
    std::cout << ">> GetCachedLineage: " <<
                      EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
                      << " table: " << table << std::endl;
  switch (lop_info->type) {
   case LogicalOperatorType::LOGICAL_PROJECTION:
   case LogicalOperatorType::LOGICAL_TOP_N:
   case LogicalOperatorType::LOGICAL_ORDER_BY:
   case LogicalOperatorType::LOGICAL_FILTER: {
      if (!lop_info->materializes_lineage) return children_opid[0];
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      glineage.emplace_back();
      idx_t total_count = LineageState::partitioned_store_buf[table]->get_total_count();
      LineageState::partitioned_store_buf[table]->fill_lineage(true, glineage[0], total_count);
      lop_info->n_output = glineage[0].size();
      if (LineageState::debug)
        std::cout << "filter/order/topn: |glineage[0]|: " << glineage[0].size() << " " << total_count << std::endl;
      return opid;
   } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      glineage.resize(2);
      idx_t total_count = LineageState::partitioned_store_buf[table]->get_total_count();
      for (idx_t i=0; i < 2; ++i) {
        if (i == 1) {
          LineageState::partitioned_store_buf[table]->fill_lineage(false, glineage[1], total_count);
        } else {
          LineageState::partitioned_store_buf[table]->fill_lineage(true, glineage[0], total_count);
        }
      }
      lop_info->n_output = glineage[0].size();
      if (LineageState::debug)
        std::cout << " |glineage[0]|: " << glineage[0].size() << " |glineage[1]|: "
                                     << glineage[1].size() << " " << total_count << std::endl;
      return opid;
   } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      vector<vector<idx_t>>& glineage = LineageState::lineage_global_store[table];
      idx_t total_count = LineageState::partitioned_store_buf[table]->fill_list_lineage(glineage);
      lop_info->n_output = glineage.size();
      lop_info->n_input = total_count;
      if (LineageState::debug) {
          std::cout << "total_count: " << total_count << std::endl;
          std::cout << " |glineage|: " << glineage.size() << std::endl;
      }

      return opid;
   } default: {}}
  
  return 10000000;
}

}
