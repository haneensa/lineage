#include "lineage/lineage_meta.hpp"

#include "lineage/lineage_init.hpp"
#include <iostream>
#include <string>
#include <sstream>
#include <iomanip>  // for setw, setfill


// output the plan
namespace duckdb {

std::string escape_json(const std::string &s) {
    std::ostringstream o;
    for (auto c = s.cbegin(); c != s.cend(); c++) {
        switch (*c) {
            case '"': o << "\\\""; break;
            case '\\': o << "\\\\"; break;
            case '\b': o << "\\b"; break;
            case '\f': o << "\\f"; break;
            case '\n': o << "\\n"; break;
            case '\r': o << "\\r"; break;
            case '\t': o << "\\t"; break;
            default:
                if ('\x00' <= *c && *c <= '\x1f') {
                    o << "\\u"
                      << std::hex << std::setw(4) << std::setfill('0') << (int)*c;
                } else {
                    o << *c;
                }
        }
    }
    return o.str();
}

std::string serialize_to_json(idx_t qid, idx_t root) {
  auto &lop = LineageState::qid_plans[qid][root];
  
  std::ostringstream oss;
  oss << "{";
  oss << "\"opid\": " << root << ",";
  oss << "\"name\": \"" << escape_json(EnumUtil::ToChars<LogicalOperatorType>(lop->type)) << "\",";
  oss << "\"sink_id\": " << lop->sink_id << ","; // the first ancestor with has_lineage set or NULL if root
  oss << "\"source_id\": [";
  for (size_t i = 0; i < lop->source_id.size(); i++) {
    if (i > 0) oss << ",";
    oss << lop->source_id[i];
  }
  oss << "],"; // the first child with has_lineage (extension) or leaf node
  oss << "\"table\": \"" << escape_json(lop->table_name) << "\",";
  oss << "\"source_table\": [";
  for (size_t i = 0; i < lop->source_id.size(); i++) {
    if (i > 0) oss << ",";
    oss << "\"";
    string src_table = LineageState::qid_plans[qid][lop->source_id[i]]->table_name;
    oss << escape_json(src_table);
    oss << "\""; 
  }  
  oss <<  "],"; // source node table_name // if not scan then the op type
  oss << "\"has_lineage\": " << (lop->has_lineage ? "true" : "false") << ",";
  oss << "\"children\": [";
  for (size_t i = 0; i < lop->children.size(); i++) {
    if (i > 0) oss << ",";
    oss << serialize_to_json(qid, lop->children[i]);
  }
  oss << "]";
  oss << "}";
  
  return oss.str();
}


void LineageMetaFunction::LineageMetaImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->CastNoConst<LineageMetaBindData>();
  output.SetCardinality(0);

  idx_t count = LineageState::qid_plans_roots.size();
  idx_t limit = count % STANDARD_VECTOR_SIZE;
  idx_t start = bind_data.offset;

  if (start >= count) return;

  output.data[0].Sequence(start, 1, limit);
  for (idx_t i=start; i < limit+start; i++) {
    idx_t root = LineageState::qid_plans_roots[i];
    string plan_json = serialize_to_json(i, root);
    output.data[1].SetValue(i, Value(plan_json));
  }
  output.SetCardinality(limit);
  bind_data.offset += limit;
}

unique_ptr<FunctionData> LineageMetaFunction::LineageMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

  auto result = make_uniq<LineageMetaBindData>();

  names.emplace_back("query_id");
  return_types.emplace_back(LogicalType::INTEGER);

  names.emplace_back("plan");
  return_types.emplace_back(LogicalType::VARCHAR);

  return std::move(result);
}

} // namespace duckdb
