#include "lineage/lineage_query.hpp"
#include <iostream>
#include <string>

namespace duckdb {

std::pair<int, int> LocateChunk(string& table_name, idx_t oid) {
  idx_t offset = 0;
  idx_t index = 0;
  for (auto& p: LineageState::lineage_store[ table_name ] ) {
    if (oid >= offset && oid < offset + p.second) break;
    ++index;
    offset += p.second;
  }
  if (LineageState::lineage_store[ table_name ].size() <= index ) return {-1, 0};
  return {index, offset};
}

// 1) replace semi with regular join, then put an aggregation on top to join on the left side
// use this to combine multiple attributes into prov poly structure
void fill_iids_per_partition(bool nested, string table_name,
    vector<idx_t>& oids, shared_ptr<SourceContext>& sc) {

  if (!nested)  sc->iids.reserve(oids.size());
  for (auto& oid : oids) {
    // 1) find the chunk_id
    // todo: do this once for all oids; group by cid, offset -> [oids] 
    pair<int, int> cid_offset = LocateChunk(table_name, oid);
    int chunk_id = cid_offset.first;
    int offset = cid_offset.second;
    int local_oid = oid - offset;
    //std::cout << table_name << " " << nested << " " << oid << " " << chunk_id << " " << offset << " " << local_oid << std::endl;
    if (chunk_id == -1) break;

    idx_t count = LineageState::lineage_store[table_name][chunk_id].second;
    Vector& payload = LineageState::lineage_store[ table_name ][chunk_id].first;
    if (nested) { // fill iids
      // Get child vector (holds all BIGINTs)
      Vector &child_vector = ListVector::GetEntry(payload);
      // Get offsets and lengths for the parent LIST vector
      auto *entries = ListVector::GetData(payload); // this gives list_entry_t*
      auto entry = entries[local_oid];
      auto offset = entry.offset;
      auto length = entry.length;

      // Get raw BIGINT data from child vector
      auto *child_data = FlatVector::GetData<int64_t>(child_vector);
      sc->iids.resize(length);

      for (idx_t i = 0; i < length; i++) {
          sc->iids[i] = child_data[offset + i];
      }
      // std::cout << offset << " "<< length << std::endl;
      // ith entry has n element; this way i can tell from iids which ith it belong to
      sc->zone_map.emplace_back(length);
    } else { // fill iids_per_partition
      idx_t iid = payload.GetValue(local_oid).GetValue<int64_t>();
      sc->iids.emplace_back(iid);
    }
  }
}

void LQ(idx_t qid, idx_t root,
        vector<idx_t>& oids_per_partition,
        unordered_map<idx_t, vector<shared_ptr<SourceContext>>>& sources) {
  auto &lop = LineageState::qid_plans[qid][root];
  bool debug = false;
  if (lop->has_lineage) {
    string name = EnumUtil::ToChars<LogicalOperatorType>(lop->type);
    bool nested = lop->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY
                        || lop->type == LogicalOperatorType::LOGICAL_DELIM_GET;
    string sink_table = lop->table_name.empty() ? name : lop->table_name;
    int sink_opid = lop->sink_id;
    int src_opid = lop->source_id[0];
    string src_table = LineageState::qid_plans[qid][src_opid]->table_name;
    auto src_type = LineageState::qid_plans[qid][src_opid]->type;
    string src_name = EnumUtil::ToChars<LogicalOperatorType>(src_type);
    src_table = src_table.empty() ? src_name : src_table;
    if (debug) std::cout << name << " LEFT: " << sink_table << " " << sink_opid << " " << src_table
      << " " << src_opid << " " << src_name << std::endl;

    string table_name = to_string(qid) + "_" + to_string(root);
    bool is_leaf = lop->children.size() == 0
                  || src_type == LogicalOperatorType::LOGICAL_GET;
    shared_ptr<SourceContext> sc = make_shared_ptr<SourceContext>(sink_opid, nested);
    fill_iids_per_partition(nested, table_name, oids_per_partition, sc);
    if (is_leaf) {
      // std::cout << sc->iids.size() << std::endl;
      sources[src_opid].emplace_back(std::move(sc));
    } else {
      if (!sc->iids.empty())  LQ(qid, lop->children[0], sc->iids, sources);
    }

    if (lop->children.size() > 1) {
      src_opid = lop->source_id[1];
      src_table = LineageState::qid_plans[qid][src_opid]->table_name;
      auto src_type = LineageState::qid_plans[qid][src_opid]->type;
      src_name = EnumUtil::ToChars<LogicalOperatorType>(src_type);
      //bool is_leaf = src_type == LogicalOperatorType::LOGICAL_GET; 
      bool is_leaf = lop->children.size() < 1
                  || src_type == LogicalOperatorType::LOGICAL_GET;
      src_table = src_table.empty() ? src_name : src_table;
      if (debug) std::cout << name << " RIGHT: " << sink_table << " " << sink_opid << " " << src_table
        << " " << src_opid << " " << src_name << std::endl;
      shared_ptr<SourceContext> sc_right = make_shared_ptr<SourceContext>(sink_opid, nested);
      if (lop->join_type == JoinType::RIGHT_SEMI) {
        if (is_leaf) {
          sc_right->iids = std::move(oids_per_partition);
          sources[src_opid].emplace_back(std::move(sc_right));
        } else {
          LQ(qid, lop->children[1], oids_per_partition, sources);
        }
        return;
      } 
      fill_iids_per_partition(nested, table_name + "_right", oids_per_partition, sc_right);
      if (is_leaf) {
        // std::cout << sc_right->iids.size() << std::endl;
        sources[src_opid].emplace_back(std::move(sc_right));
      } else {
        if (!sc_right->iids.empty()) LQ(qid, lop->children[1], sc_right->iids, sources);
      }
    }
  } else {
    // pass until has_lineage == true
    for (size_t i = 0; i < lop->children.size(); i++) {
      LQ(qid, lop->children[i], oids_per_partition, sources);
    }
  }
}

}
