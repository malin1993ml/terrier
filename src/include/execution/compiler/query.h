#pragma once

#include <string>

#include "execution/compiler/code_context.h"
#include "execution/compiler/query_state.h"
#include "execution/util/region.h"

namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::ast {
class BlockStmt;
}

namespace tpl::compiler {

class Query {
 public:
  Query(const terrier::planner::AbstractPlanNode &node) : node_(node), region_("QueryRegion"), code_ctx_(&region_),
                                                          query_state_(&region_), compiled_fn_(nullptr) {}

  const terrier::planner::AbstractPlanNode &GetPlan() { return node_; }
  CodeContext *GetCodeContext() { return &code_ctx_; }
  QueryState *GetQueryState() { return &query_state_; }
  util::Region *GetRegion() { return &region_; }
  const std::string &GetQueryStateName() { return name_qs; }
  const std::string &GetQueryInitName() { return name_qinit; }

  void SetCompiledFunction(ast::BlockStmt *fn) { compiled_fn_ = fn; }
  ast::BlockStmt *GetCompiledFunction() { return compiled_fn_; }

 private:
  std::string name_qs = "query_state";
  std::string name_qinit = "query_state";
  const terrier::planner::AbstractPlanNode &node_;
  util::Region region_;
  CodeContext code_ctx_;
  QueryState query_state_;
  ast::BlockStmt *compiled_fn_;
};

}