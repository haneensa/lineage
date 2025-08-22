// TODO: create table that stores templates. user could add to the template (compile to c++) then use it to evaluate
//       prov poly
// EvalPoly(string template_name, string annotations)
// count: mul -> *, sum -> +, type(annotations) ->int
// prov poly string: mul -> X(a,c), + -> +( X(a,c) + X(a,d) ), type(annoations)-> string
// sum: mul -> *, sum -> +, annotations -> column_id
// avg: count, sum, annotations -> column_id
// etc..
// probs
// lineage: vector<vector<idx_t>> sources; 
// str and lineage init annotations with rowid

#include "fade/prov_poly_eval.hpp"
#include "fade/fade.hpp"

#include "lineage/lineage_init.hpp"
#include <iostream>
#include <string>
#include <type_traits>


namespace duckdb {

using ann_type = string; 
// using ann_type = std::unordered_map<int, std::vector<int>>;

struct PolyEvalBindData : public TableFunctionData {
  explicit PolyEvalBindData() : qid(0), model("formula") {}
  idx_t qid;
  string model;
};

struct PolyEvalGlobalState : public GlobalTableFunctionState {
    explicit PolyEvalGlobalState(vector<int> annotation_p, vector<string> formula_p) :
    annotation(std::move(annotation_p)), formula(std::move(formula_p)), offset(0) {}
    vector<string> formula;
    vector<int> annotation;
    idx_t offset;
};


template <typename T>
void filter_eval(const vector<idx_t>& lineage,
                 const vector<T>& in_ann, vector<T>& out_ann) {
  for (idx_t i=0; i < lineage.size(); ++i)
    out_ann[i] = in_ann[lineage[i]];
}

template <typename T>
void join_eval(const vector<idx_t>& lhs_lineage, const vector<idx_t>& rhs_lineage,
               const vector<T>& lhs_ann, const vector<T>& rhs_ann, vector<T>& out_ann) {
  for (idx_t i=0; i < lhs_lineage.size(); ++i)
    out_ann[i] = lhs_ann[lhs_lineage[i]] * rhs_ann[rhs_lineage[i]];
}

template <>
void join_eval<string>(const vector<idx_t>& lhs_lineage, const vector<idx_t>& rhs_lineage,
               const vector<string>& lhs_ann, const vector<string>& rhs_ann, vector<string>& out_ann) {
  for (idx_t i=0; i < lhs_lineage.size(); ++i)
    out_ann[i] = "[" + lhs_ann[lhs_lineage[i]] + "." +  rhs_ann[rhs_lineage[i]] + "]";
}

// if lineage
// unordered_map<int, vector<idx_t>> out_sources
// for each src in lhs_out_sources:
//  out_sources[src][i] = lhs_out_sources[src][lhs_lineage[i]]
//
// lineage: out_sources[0] = lhs_out_sources[0]; out_sources[1] = rhs_out_sources[0];


template <typename T>
void agg_eval(const vector<vector<idx_t>>& bw_lineage,
               const vector<T>& in_ann, vector<T>& out_ann) {

  for (idx_t o=0; o < bw_lineage.size(); ++o) {
    for (idx_t i=0; i < bw_lineage[o].size(); ++i) {
      idx_t iid = bw_lineage[o][i];
      out_ann[o] += in_ann[iid];
    }
  }
}

template <>
void agg_eval<string>(const vector<vector<idx_t>>& bw_lineage,
               const vector<string>& in_ann, vector<string>& out_ann) {

  for (idx_t o=0; o < bw_lineage.size(); ++o) {
    out_ann[o] += "(";
    for (idx_t i=0; i < bw_lineage[o].size(); ++i) {
      idx_t iid = bw_lineage[o][i];
      out_ann[o] += ( in_ann[iid] + "+" );
    }
    out_ann[o] += ")";
  }
}

template <typename T>
void scan_eval(vector<T>& out_ann, idx_t count, string table) {
  out_ann.assign(count, 1);
}

template <>
void scan_eval<string>(vector<string>& out_ann, idx_t count, string table) {
  out_ann.resize(count);
  for (idx_t i=0; i < count; ++i) {
    out_ann[i] = table + to_string(i);
  }
}

template <typename T>
idx_t PolyEval(ClientContext &context, string model, idx_t qid, idx_t opid, idx_t wid,
              std::unordered_map<idx_t, vector<T>>& annotations_per_opid) {
  auto &lop_info = LineageState::qid_plans[qid][opid];
  string ltable = to_string(qid) + "_" + to_string(opid);
  
  vector<idx_t> children_opid;
  for (auto &child : lop_info->children) {
     idx_t cid = PolyEval(context, model, qid, child, wid, annotations_per_opid);
     children_opid.push_back(cid);
  }
  
  if (LineageState::debug)
    std::cout << ">> PolyEval: " << EnumUtil::ToChars<LogicalOperatorType>(lop_info->type)
                                 << " table: " << ltable << " |c|: " << children_opid.size() << std::endl;

  switch (lop_info->type) {
    case LogicalOperatorType::LOGICAL_GET: {
      // read in the annotations into fnode->ann
      auto conn = make_uniq<Connection>(*context.db);
      string query = "select count(*) from " + lop_info->table_name;
      auto result = conn->Query(query);
      if (!result || result->HasError()) {
          std::cerr << "Query failed: " << (result ? result->GetError() : "null result") << std::endl;
      }
      idx_t count = result->GetValue(0, 0).GetValue<idx_t>();
      std::cout << lop_info->table_name << " " << query << " " << count << std::endl;
      scan_eval(annotations_per_opid[opid], count, lop_info->table_name);
      return opid;
    } case LogicalOperatorType::LOGICAL_PROJECTION: {
      return children_opid[0];
    } case LogicalOperatorType::LOGICAL_FILTER:
      case LogicalOperatorType::LOGICAL_ORDER_BY: {
      if (!lop_info->has_lineage) return children_opid[0];
      vector<idx_t>& lineage = LineageState::lineage_global_store[ltable][0];
      auto& in_ann =  annotations_per_opid[children_opid[0]];
      auto& out_ann = annotations_per_opid[opid];
      out_ann.resize(lineage.size());
      filter_eval(lineage, in_ann, out_ann);
      return opid;
    } case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      vector<idx_t>& lhs_lineage = LineageState::lineage_global_store[ltable][0];
      vector<idx_t>& rhs_lineage = LineageState::lineage_global_store[ltable][1];
      auto& lhs_ann =  annotations_per_opid[children_opid[0]];
      auto& rhs_ann =  annotations_per_opid[children_opid[1]];
      auto& out_ann =  annotations_per_opid[opid];
      std::cout << lhs_lineage.size() << " " << rhs_lineage.size()
        << " " << lhs_ann.size() << " " << rhs_ann.size() << " " << out_ann.size()
        << " " << children_opid[0] << " " << children_opid[1] << std::endl;
      out_ann.resize(lhs_lineage.size());
      join_eval(lhs_lineage, rhs_lineage, lhs_ann, rhs_ann, out_ann);
      
      // if (lineage)
      // unordered_map<int, vector<idx_t>> out_sources
      // for each src in lhs_out_sources:
      //  filter_eval(lhs_lineage, lhs_out_sources[src], out_sources[src]);
      return opid;
    } case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      auto& in_ann =  annotations_per_opid[children_opid[0]];
      auto& out_ann =  annotations_per_opid[opid];
      vector<vector<idx_t>>& lineage = LineageState::lineage_global_store[ltable];
      out_ann.resize(lineage.size());
      agg_eval(lineage, in_ann, out_ann);

      // if (lineage)
      // for each src in out_sources:
      //    accumelate in a set
      return opid;
    } default: {}}

  return 10000000;
}

unique_ptr<FunctionData> PolyEvalFunction::PolyEvalBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
  // TODO: set output of eval to the type based on the annotationi and SUM and MUL operations over them
  auto result = make_uniq<PolyEvalBindData>();
  result->qid = input.inputs[0].GetValue<idx_t>();
  result->model = "formula";

  names.emplace_back("out_rowid");
  return_types.emplace_back(LogicalType::ROW_TYPE);

  names.emplace_back("count");
  return_types.emplace_back(LogicalType::INTEGER);

  names.emplace_back("formula");
  return_types.emplace_back(LogicalType::VARCHAR);
  
  return std::move(result);
}

void PolyEvalFunction::PolyEvalImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->CastNoConst<PolyEvalBindData>();
  idx_t count = 0;
  // if eval type is string, else if int, else ?
  auto &gstate = data_p.global_state->Cast<PolyEvalGlobalState>();
    
  idx_t remaining = gstate.annotation.size() - gstate.offset;
  count = remaining  > STANDARD_VECTOR_SIZE ? STANDARD_VECTOR_SIZE : remaining;
    
  output.data[0].Sequence(gstate.offset, 1, count);

  data_ptr_t ptr = (data_ptr_t)(gstate.annotation.data() + gstate.offset);
  Vector in_index(LogicalType::INTEGER, ptr);
  output.data[1].Reference(in_index);
    
  auto &vec = output.data[2];
  auto data = FlatVector::GetData<string_t>(vec);
  for (idx_t i = 0; i < gstate.formula.size(); ++i) {
    data[i] = StringVector::AddString(vec, gstate.formula[i]);
  }
  
  gstate.offset += count;
  output.SetCardinality(count);
}


unique_ptr<GlobalTableFunctionState> PolyEvalFunction::PolyEvalInit(ClientContext &context,
                                                 TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->CastNoConst<PolyEvalBindData>();
  idx_t last_qid = LineageState::qid_plans_roots.size()-1;
  idx_t root_id = LineageState::qid_plans_roots[last_qid];
  
  std::unordered_map<idx_t, vector<string>> formula_per_opid;
  std::unordered_map<idx_t, vector<int>> annotations_per_opid;
  PolyEval(context, bind_data.model, last_qid, root_id, 0 /*wid*/, annotations_per_opid);
  idx_t out_opid = PolyEval(context, bind_data.model, last_qid, root_id, 0 /*wid*/, formula_per_opid);

  if (LineageState::debug) std::cout << "root -> " << out_opid << std::endl;
  return make_uniq<PolyEvalGlobalState>(std::move(annotations_per_opid[out_opid]),
      std::move(formula_per_opid[out_opid]));
}

} // namespace duckdb
