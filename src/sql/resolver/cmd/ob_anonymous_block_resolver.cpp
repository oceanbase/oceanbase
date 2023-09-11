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
  void *params_buf = NULL;
  if (OB_ISNULL(session_info_)
      || OB_ISNULL(params_.param_list_)
      || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Argument is NULL",
             K(session_info_), K(params_.param_list_), K(allocator_), K(ret));
  } else if (OB_ISNULL(stmt = create_stmt<ObAnonymousBlockStmt>())
             || OB_ISNULL(params_buf = allocator_->alloc(sizeof(ParamStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create index stmt failed", K(ret));
  } else{
    ParamStore *param_store = new(params_buf)ParamStore( ObWrapperAllocator(*allocator_) );
    stmt->set_params(param_store);
    stmt_ = stmt;
  }
  CK (OB_NOT_NULL(stmt));
  if (OB_SUCC(ret)) {
    if (params_.is_prepare_protocol_) {
      stmt->set_prepare_protocol(true);
      stmt->set_stmt_id(params_.statement_id_);
      stmt->set_sql(params_.cur_sql_);
      if ((params_.is_prepare_stage_
            || params_.is_pre_execute_)
          && OB_ISNULL(session_info_->get_pl_context())) {
        CK (OB_LIKELY(T_SP_ANONYMOUS_BLOCK == parse_tree.type_));
        CK (OB_NOT_NULL(block_node = parse_tree.children_[0]));
        CK (OB_LIKELY(T_SP_BLOCK_CONTENT == block_node->type_
                      || T_SP_LABELED_BLOCK == block_node->type_));
        OZ (resolve_anonymous_block(*block_node, *stmt, true));
        if (OB_SUCC(ret) && params_.is_pre_execute_) {
          OZ (add_param());
        }
      } else {
        if (params_.is_prepare_stage_) {
          CK (OB_LIKELY(T_SP_ANONYMOUS_BLOCK == parse_tree.type_));
          CK (OB_NOT_NULL(block_node = parse_tree.children_[0]));
          CK (OB_LIKELY(T_SP_BLOCK_CONTENT == block_node->type_
                        || T_SP_LABELED_BLOCK == block_node->type_));
          OZ (resolve_anonymous_block(*block_node, *stmt, false));
        }
        OZ (add_param());
      }
    } else if (OB_UNLIKELY(T_SP_ANONYMOUS_BLOCK != parse_tree.type_
                           || OB_ISNULL(block_node = parse_tree.children_[0]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the children of parse tree is NULL", K(ret));
    } else if (T_SP_BLOCK_CONTENT != block_node->type_
               && T_SP_LABELED_BLOCK != block_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid procedure name node", K(block_node->type_), K(ret));
    } else {
      CK (OB_LIKELY(T_SP_ANONYMOUS_BLOCK == parse_tree.type_));
      CK (OB_NOT_NULL(block_node = parse_tree.children_[0]));
      CK (OB_LIKELY(T_SP_BLOCK_CONTENT == block_node->type_
                    || T_SP_LABELED_BLOCK == block_node->type_));
      OX (stmt->set_prepare_protocol(false));
      OX (stmt->set_body(block_node));
      OZ (add_param());
    }
  }
  return ret;
}

int ObAnonymousBlockResolver::resolve_anonymous_block(
  ParseNode &block_node, ObAnonymousBlockStmt &stmt, bool resolve_inout_param)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(pl::ObPLFunctionAST, func_ast, *(params_.allocator_)) {
    ParamStore param_list( ObWrapperAllocator(*(params_.allocator_)) );
    const ParamStore *p_param_list = (params_.param_list_ != NULL && params_.param_list_->count() > 0)
        ? (params_.param_list_) : &param_list;
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
                              p_param_list);
    for (int64_t i = 0; OB_SUCC(ret) && i < params_.query_ctx_->question_marks_count_; ++i) {
      ObObjParam param = ObObjParam(ObObj(ObNullType));
      const_cast<ObObjMeta&>(param.get_null_meta()).reset();
      OZ (param_list.push_back(param));
    }
    OZ (package_guard.init());
    OX (func_ast.set_db_name(params_.session_info_->get_database_name()));
    OZ (pl::ObPLCompiler::init_anonymous_ast(
          func_ast,
          *(params_.allocator_),
          *(params_.session_info_),
          *(params_.sql_proxy_),
          *(params_.schema_checker_->get_schema_guard()),
          package_guard,
          p_param_list));
    OZ (resolver.init(func_ast));
    OZ (resolver.resolve_root(&block_node, func_ast));
    for (int64_t i = 0; OB_SUCC(ret) && i < func_ast.get_dependency_table().count(); ++i) {
      OZ (stmt.add_global_dependency_table(func_ast.get_dependency_table().at(i)));
    }
    
    const pl::ObPLSymbolTable &symbol_table = func_ast.get_symbol_table();
    for (int64_t i = 0;
        OB_SUCC(ret) && resolve_inout_param && i < func_ast.get_arg_count(); ++i) {
      const pl::ObPLVar *symbol = symbol_table.get_symbol(i);
      CK (OB_NOT_NULL(symbol));
      if (OB_FAIL(ret)) {
      } else if (!symbol->is_readonly()) {
        OZ (stmt.get_out_idx().add_member(i));
      }
    }
  }
  return ret;
}

int ObAnonymousBlockResolver::add_param()
{
  int ret = OB_SUCCESS;
  ObAnonymousBlockStmt *anonymous_stmt = NULL;
  if (OB_ISNULL(stmt_)
      || OB_ISNULL(params_.param_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is NULL", K(stmt_), K(params_.param_list_), K(ret));
  } else {
    anonymous_stmt = static_cast<ObAnonymousBlockStmt*>(stmt_);
  }
  CK (OB_NOT_NULL(anonymous_stmt));
  if (OB_FAIL(ret)) {
  } else if (params_.param_list_->count() > 0) {
    CK (params_.param_list_->count() == params_.query_ctx_->question_marks_count_);
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
