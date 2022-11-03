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
#include "ob_drop_routine_resolver.h"
#include "ob_drop_routine_stmt.h"
#include "ob_drop_func_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
namespace sql
{
int ObDropProcedureResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *name_node = NULL;
  ObString db_name;
  ObString sp_name;
  ObDropRoutineStmt *proc_stmt = NULL;
  if (OB_UNLIKELY(parse_tree.type_ != T_SP_DROP)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ != 1)
      || OB_ISNULL(name_node = parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse tree is invalid", "type", get_type_name(parse_tree.type_),
             K_(parse_tree.children), K_(parse_tree.num_child), K(name_node));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is null");
  } else if (OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker is null");
  } else if (OB_FAIL(ObResolverUtils::resolve_sp_name(*session_info_, *name_node, db_name, sp_name))) {
    LOG_WARN("resolve sp name failed", K(ret));
  } else if (OB_ISNULL(proc_stmt = create_stmt<ObDropRoutineStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create drop procedure stmt failed");
  } else if (ObSchemaChecker::is_ora_priv_check()
             && OB_FAIL(schema_checker_->check_ora_ddl_priv(
                                              session_info_->get_effective_tenant_id(),
                                              session_info_->get_priv_user_id(),
                                              db_name,
                                              stmt::T_DROP_ROUTINE,
                                              session_info_->get_enable_role_array()))){
    LOG_WARN("failed to check privilege in drop routine", K(ret));
  }
  else {
    obrpc::ObDropRoutineArg &routine_arg = proc_stmt->get_routine_arg();
    routine_arg.tenant_id_ = session_info_->get_effective_tenant_id();
    routine_arg.db_name_ = db_name;
    routine_arg.routine_name_ = sp_name;
    routine_arg.routine_type_ = share::schema::ROUTINE_PROCEDURE_TYPE;
    routine_arg.if_exist_ = parse_tree.value_;
  }
  return ret;
}

int ObDropFunctionResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *name_node = NULL;
  ObString db_name;
  ObString sp_name;
  ObDropRoutineStmt *routine_stmt = NULL;
  bool if_exist = false;
  bool pl_y = true;
  bool need_try_pl_function = true;
  // 因为mysql.y和pl_mysql.y的if exists语法结构不一样，mysql.y的已经被很多用了，
  // 没办法改成一致，暂时只能写成两套了。
  if (parse_tree.type_ == T_DROP_FUNC) {
    if (OB_ISNULL(parse_tree.children_)
        || OB_UNLIKELY(parse_tree.num_child_ != 2)
        || OB_ISNULL(name_node = parse_tree.children_[1])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("parse tree is invalid", "type", get_type_name(parse_tree.type_),
               K_(parse_tree.children), K_(parse_tree.num_child), K(name_node));
    } else {
      if_exist = (NULL != parse_tree.children_[0]);
      sp_name = ObString(parse_tree.children_[1]->str_len_, parse_tree.children_[1]->str_value_);
      pl_y = false;
    }
  } else if (parse_tree.type_ == T_SF_DROP) {
    if (OB_ISNULL(parse_tree.children_)
        || OB_UNLIKELY(parse_tree.num_child_ != 1)
        || OB_ISNULL(name_node = parse_tree.children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("parse tree is invalid", "type", get_type_name(parse_tree.type_),
               K_(parse_tree.children), K_(parse_tree.num_child), K(name_node));
    } else {
      if_exist = parse_tree.value_;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse tree is invalid", "type", get_type_name(parse_tree.type_),
             K_(parse_tree.children), K_(parse_tree.num_child), K(name_node));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(session_info_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("session info is null");
    } else if (pl_y && OB_FAIL(ObResolverUtils::resolve_sp_name(*session_info_, *name_node, db_name, sp_name))) {
      ret = OB_ERR_NO_DB_SELECTED == ret && lib::is_mysql_mode() ? OB_SUCCESS : ret;
      LOG_WARN("resolve sp name failed", K(ret));
    }
    // drop ddl function 和 drop pl function公用语法, 需要先检查是否是ddl function
    if (OB_FAIL(ret)) {
    } else if (pl_y
               && OB_LIKELY(2 == name_node->num_child_)
               && OB_ISNULL(name_node->children_[0])
               && lib::is_mysql_mode()) {
      bool exist = false;
      const share::schema::ObUDF *udf_info = nullptr;
      ObString lower_name;
      OZ (ob_write_string(*allocator_, sp_name, lower_name));
      OX (ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, lower_name));
      OZ (schema_checker_->get_udf_info(
        session_info_->get_effective_tenant_id(), lower_name, udf_info, exist));
      if (OB_FAIL(ret)) {
      } else if (exist) {
        ObDropFuncStmt *drop_func_stmt = NULL;
        CK (OB_NOT_NULL(drop_func_stmt = create_stmt<ObDropFuncStmt>()));
        if (OB_SUCC(ret)) {
          obrpc::ObDropUserDefinedFunctionArg &drop_func_arg = drop_func_stmt->get_drop_func_arg();
          drop_func_arg.tenant_id_ = session_info_->get_effective_tenant_id();
          drop_func_arg.name_ = lower_name;
          drop_func_arg.if_exist_ =  if_exist;
          need_try_pl_function = false;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!need_try_pl_function) {
      // do nothing ...
    } else if (OB_ISNULL(routine_stmt = create_stmt<ObDropRoutineStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create drop function stmt failed");
    } else {
      obrpc::ObDropRoutineArg &routine_arg = routine_stmt->get_routine_arg();
      routine_arg.tenant_id_ = session_info_->get_effective_tenant_id();
      routine_arg.db_name_ = pl_y ? db_name : session_info_->get_database_name();
      routine_arg.routine_name_ = sp_name;
      routine_arg.routine_type_ = share::schema::ROUTINE_FUNCTION_TYPE;
      routine_arg.if_exist_ = if_exist;
    }
  }

  return ret;
}
}
}



