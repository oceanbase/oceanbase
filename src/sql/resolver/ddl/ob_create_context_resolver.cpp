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

#define USING_LOG_PREFIX  SQL_RESV

#include "sql/resolver/ddl/ob_create_context_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/ddl/ob_context_stmt.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_select_stmt_printer.h"
#include "sql/session/ob_sql_session_info.h"

#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "lib/hash/ob_hashset.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{
ObCreateContextResolver::ObCreateContextResolver(ObResolverParams &params) : ObDDLResolver(params)
{
}

ObCreateContextResolver::~ObCreateContextResolver()
{
}

int ObCreateContextResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateContextStmt *stmt = NULL;
  bool is_sync_ddl_user = false;
  if (OB_UNLIKELY(T_CREATE_CONTEXT != parse_tree.type_)
      || OB_UNLIKELY(ROOT_NUM_CHILD != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse_tree", K(parse_tree.type_), K(parse_tree.num_child_), K(ret));
  } else if (OB_ISNULL(parse_tree.children_) || OB_ISNULL(parse_tree.children_[CONTEXT_NAMESPACE])
             || OB_ISNULL(parse_tree.children_[TRUSTED_PACKAGE_NAME])
             || OB_ISNULL(parse_tree.children_[ACCESSED_TYPE])
             || OB_ISNULL(allocator_) || OB_ISNULL(session_info_)
             || OB_ISNULL(params_.query_ctx_)
             || (2 != parse_tree.children_[TRUSTED_PACKAGE_NAME]->num_child_)
             || (OB_ISNULL(parse_tree.children_[TRUSTED_PACKAGE_NAME]->children_[1]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parse_tree.children_),
                                    K(parse_tree.children_[CONTEXT_NAMESPACE]),
                                    K(parse_tree.children_[TRUSTED_PACKAGE_NAME]),
                                    K(parse_tree.children_[ACCESSED_TYPE]),
                                    K(allocator_), K(session_info_),
                                    K(params_.query_ctx_));
  } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
    LOG_WARN("Failed to check sync_dll_user", K(ret));
  } else if (OB_UNLIKELY(NULL == (stmt = create_stmt<ObCreateContextStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create context stmt failed", K(ret));
  } else {
    ObString schema_name;
    ObString ctx_namespace;
    ObString package_name;
    stmt_ = stmt;
    ObContextDDLArg &create_arg = stmt->get_arg();
    ObContextSchema &ctx_schema = create_arg.ctx_schema_;
    create_arg.or_replace_ = NULL != parse_tree.children_[OR_REPLACE_NODE];
    ctx_schema.set_tenant_id(session_info_->get_effective_tenant_id());
    //ctx_schema.set_database_id(session_info_->get_database_id());
    // check accessed type
    ctx_schema.set_context_type(static_cast<ObContextType>
                                     (parse_tree.children_[ACCESSED_TYPE]->value_));
    // check database name
    if (nullptr == parse_tree.children_[TRUSTED_PACKAGE_NAME]->children_[0]) {
      schema_name = session_info_->get_database_name();
    } else if (OB_FAIL(resolve_context_namespace(*parse_tree.children_[TRUSTED_PACKAGE_NAME]->children_[0],
                                                  schema_name))) {
      LOG_WARN("failed to resolve database name", K(ret));
    }
    // check namesapce && package_name
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(resolve_context_namespace(*parse_tree.children_[CONTEXT_NAMESPACE],
                                                  ctx_namespace))) {
      LOG_WARN("failed to resolve namespace", K(ret));
    } else if (OB_FAIL(resolve_context_namespace(*parse_tree.children_[TRUSTED_PACKAGE_NAME]->children_[1],
                                                  package_name))) {
      LOG_WARN("failed to resolve package name", K(ret));
    } else if (OB_FAIL(check_context_namespace(ctx_namespace))) {
      LOG_WARN("failed to check ctx name", K(ret));
    } else if (OB_FAIL(ctx_schema.set_namespace(ctx_namespace))
               || OB_FAIL(ctx_schema.set_trusted_package(package_name))
               || OB_FAIL(ctx_schema.set_schema_name(schema_name))) {
      LOG_WARN("failed to set context info", K(ret));
    }

    // 权限添加需要拿到完整stmt信息，慎重调整本段代码位置
    /*if (OB_SUCC(ret) && !(is_sync_ddl_user && session_info_->is_inner())
        && OB_FAIL(check_privilege_needed(*stmt, *select_stmt, is_force_view))) {
      LOG_WARN("fail to check privilege needed", K(ret));
    }*/

    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ (schema_checker_->check_ora_ddl_priv(
            session_info_->get_effective_tenant_id(),
            session_info_->get_priv_user_id(),
            session_info_->get_database_name(),
            OB_INVALID_ID,
            static_cast<uint64_t>(ObObjectType::CONTEXT),
            stmt::T_CREATE_CONTEXT,
            session_info_->get_enable_role_array()),
            session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
    LOG_WARN("create context", K(session_info_->get_priv_user_id()));
  }

  return ret;
}

int ObCreateContextResolver::resolve_context_namespace(const ParseNode &namespace_node,
                                                       ObString &ctx_namespace)
{
  int ret = OB_SUCCESS;
  int32_t name_len = static_cast<int32_t>(namespace_node.str_len_);
  ctx_namespace.assign_ptr(const_cast<char *>(namespace_node.str_value_), name_len);
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(ObSQLUtils::check_and_convert_context_namespace(cs_type, ctx_namespace))) {
    LOG_WARN("failed to check ctx namespace", K(ret));
  }
  return ret;
}

int ObCreateContextResolver::check_context_namespace(const ObString &ctx_namespace)
{
  int ret = OB_SUCCESS;
  ObCollationType src_cs = ObCharset::get_system_collation();
  ObCollationType dst_cs = CS_TYPE_INVALID;
  ObString in_str1 = "USERENV";
  ObString in_str2 = "CLIENTCONTEXT";
  ObString in_str3 = "SYS_";
  ObString out_str;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(dst_cs))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(sql::ObExprUtil::convert_string_collation(in_str1, src_cs,
                                                               out_str, dst_cs,
                                                               *allocator_))) {
    LOG_WARN("failed to convert string to target collation", K(ret));
  } else if (0 == out_str.case_compare(ctx_namespace)) {
    ret = OB_OBJ_ALREADY_EXIST;
    LOG_USER_ERROR(OB_OBJ_ALREADY_EXIST);
  } else if (OB_FAIL(sql::ObExprUtil::convert_string_collation(in_str2, src_cs,
                                                               out_str, dst_cs,
                                                               *allocator_))) {
    LOG_WARN("failed to convert string to target collation", K(ret));
  } else if (0 == out_str.case_compare(ctx_namespace)) {
    ret = OB_ERR_INVALID_NAMESPACE_VALUE;
    LOG_USER_ERROR(OB_ERR_INVALID_NAMESPACE_VALUE);
  } else if (OB_FAIL(sql::ObExprUtil::convert_string_collation(in_str3, src_cs,
                                                               out_str, dst_cs,
                                                               *allocator_))) {
    LOG_WARN("failed to convert string to target collation", K(ret));
  } else if (out_str.length() <= ctx_namespace.length()) {
    ObString head(out_str.length(), ctx_namespace.ptr());
    if (0 == out_str.case_compare(head)) {
      ret = OB_ERR_INVALID_NAMESPACE_BEG;
      LOG_USER_ERROR(OB_ERR_INVALID_NAMESPACE_BEG);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
