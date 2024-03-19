/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_RESV

#include "ob_anonymous_block_resolver.h"
#include "ob_anonymous_block_stmt.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_compile.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
namespace sql
{

int ObAnonymousBlockResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;

  ObAnonymousBlockStmt *stmt = NULL;
  ParseNode *block_node = NULL;
  ParamStore *param_store = NULL;

  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(params_.param_list_));
  CK (OB_NOT_NULL(allocator_));

  CK (OB_LIKELY(T_SP_ANONYMOUS_BLOCK == parse_tree.type_));
  CK (OB_NOT_NULL(block_node = parse_tree.children_[0]));
  CK (OB_LIKELY(T_SP_BLOCK_CONTENT == block_node->type_ || T_SP_LABELED_BLOCK == block_node->type_));

  if (OB_SUCC(ret)
      && (OB_ISNULL(stmt = create_stmt<ObAnonymousBlockStmt>())
          || OB_ISNULL(param_store = reinterpret_cast<ParamStore*>(allocator_->alloc(sizeof(ParamStore)))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create index stmt failed", K(ret), KP(stmt), KP(param_store));
  }

  OX (new (param_store) ParamStore( ObWrapperAllocator(*allocator_) ));
  OX (stmt->set_params(param_store));
  OX (stmt_ = stmt);

  if (OB_FAIL(ret)) {
  } else if (params_.is_prepare_protocol_) {
    stmt->set_prepare_protocol(true);
    stmt->set_stmt_id(params_.statement_id_);
    stmt->set_sql(params_.cur_sql_);
    OZ (prepare_question_mark_params());
    if (params_.is_prepare_stage_ || OB_ISNULL(session_info_->get_pl_context())) {
      OZ (resolve_anonymous_block(*block_node, *stmt));
    }
  } else {
    stmt->set_prepare_protocol(false);
    stmt->set_body(block_node);
    OZ (prepare_question_mark_params());
  }
  return ret;
}

int ObAnonymousBlockResolver::resolve_anonymous_block(ParseNode &block_node, ObAnonymousBlockStmt &stmt)
{
  int ret = OB_SUCCESS;

  HEAP_VAR(pl::ObPLFunctionAST, func_ast, *(params_.allocator_)) {
    pl::ObPLPackageGuard package_guard(params_.session_info_->get_effective_tenant_id());
    pl::ObPLResolver resolver(*(params_.allocator_),
                              *(params_.session_info_),
                              *(params_.schema_checker_->get_schema_guard()),
                              package_guard,
                              *(params_.sql_proxy_),
                              *(params_.expr_factory_),
                              NULL,
                              true,
                              false,
                              false,
                              stmt.get_params());
    OZ (package_guard.init());
    OX (func_ast.set_db_name(params_.session_info_->get_database_name()));
    OZ (pl::ObPLCompiler::init_anonymous_ast(func_ast,
                                             *(params_.allocator_),
                                             *(params_.session_info_),
                                             *(params_.sql_proxy_),
                                             *(params_.schema_checker_->get_schema_guard()),
                                             package_guard,
                                             stmt.get_params()));
    OZ (resolver.init(func_ast));
    OZ (resolver.resolve_root(&block_node, func_ast));

    for (int64_t i = 0; OB_SUCC(ret) && i < func_ast.get_dependency_table().count(); ++i) {
      OZ (stmt.add_global_dependency_table(func_ast.get_dependency_table().at(i)));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < func_ast.get_arg_count(); ++i) {
      const pl::ObPLVar *symbol = func_ast.get_symbol_table().get_symbol(i);
      CK (OB_NOT_NULL(symbol));
      if (OB_SUCC(ret) && !symbol->is_readonly()) {
        OZ (stmt.get_out_idx().add_member(i));
      }
    }
  }
  return ret;
}

int ObAnonymousBlockResolver::prepare_question_mark_params()
{
  int ret = OB_SUCCESS;
  ObAnonymousBlockStmt *anonymous_stmt = NULL;
  if (OB_ISNULL(stmt_)
      || OB_ISNULL(params_.param_list_)
      || OB_ISNULL(anonymous_stmt = static_cast<ObAnonymousBlockStmt*>(stmt_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is NULL", KPC(stmt_), KPC(params_.param_list_), KPC(anonymous_stmt), K(ret));
  } else if (params_.param_list_->count() > 0) {
    if (params_.param_list_->count() != params_.query_ctx_->question_marks_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params_list count not equal to question_marks_count", K(ret), KPC(params_.param_list_), K(params_.query_ctx_->question_marks_count_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < params_.param_list_->count(); ++i) {
      if (OB_FAIL(anonymous_stmt->add_param(params_.param_list_->at(i)))) {
        LOG_WARN("fail to push back param", K(i), K(ret));
      }
    }
  } else if (params_.query_ctx_->question_marks_count_ > 0) {
    for (int64_t i =0; OB_SUCC(ret) && i < params_.query_ctx_->question_marks_count_; ++i) {
      if (OB_FAIL(anonymous_stmt->add_param(ObObjParam(ObObj(ObNullType))))) {
        LOG_WARN("failed to push back param", K(ret), K(i));
      }
    }
  }
  return ret;
}

}
}
