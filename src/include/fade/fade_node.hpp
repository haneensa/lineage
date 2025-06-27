#pragma once

#include "fade_extension.hpp"

namespace duckdb {

enum InterventionType {
	DENSE_DELETE,
	SCALE_UNIFORM,
	SCALE_RANDOM,
	SEARCH
};

class FadeNode {
public:
  explicit FadeNode(idx_t qid, idx_t opid) : qid(qid), opid(opid), n_interventions(0), num_worker(1),
  aggid(-1), counter(0), debug(false) {}

	virtual ~FadeNode() {}
public:
  idx_t qid;
  int opid;
  int aggid;
  int n_interventions;
  int num_worker;
  int counter;
  vector<int> groups;
  
  bool debug;

  // holds post interventions output. n_output X n_interventions per worker
	std::unordered_map<string, vector<void*>> alloc_vars;
};


class FadeSparseNode : public FadeNode {
public:
  explicit FadeSparseNode(idx_t qid, idx_t opid) : FadeNode(qid, opid) {}
	virtual ~FadeSparseNode() {}
public:
	unique_ptr<int[]> annotations;
};

class FadeSingleNode: public FadeNode {
public:
  explicit FadeSingleNode(idx_t qid, idx_t opid) : FadeNode(qid, opid) {}
	virtual ~FadeSingleNode() = default; // Virtual destructor

public:
	int8_t* single_del_interventions;
};

void prepare_fade_plan(idx_t qid, idx_t opid, std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
                      unordered_map<string, vector<string>>& spec_map);
pair<int, int> get_start_end(int row_count, int thread_id, int num_worker);

} // namespace duckdb
