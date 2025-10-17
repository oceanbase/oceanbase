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

#include "sql/resolver/cmd/ob_service_name_resolver.h"

#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace sql
{
typedef ObAlterSystemResolverUtil Util;
int ObServiceNameResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObServiceNameStmt *stmt = create_stmt<ObServiceNameStmt>();
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  ObString service_name_str;
  if (OB_UNLIKELY(T_SERVICE_NAME != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_SERVICE_NAME", KR(ret), "type",
        get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (3 != parse_tree.num_child_
      || OB_ISNULL(parse_tree.children_[0])
      || OB_ISNULL(parse_tree.children_[1])
      || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree or session info", KR(ret), "num_child", parse_tree.num_child_,
        KP(parse_tree.children_[0]), KP(parse_tree.children_[1]), KP(session_info_));
  } else if (OB_FAIL(Util::get_and_verify_tenant_name(
      parse_tree.children_[2],
      false, /* allow_sys_meta_tenant */
      session_info_->get_effective_tenant_id(),
      target_tenant_id,
      "Service name related command"))) {
    LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
        K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[1]));
  } else if (OB_UNLIKELY(T_INT != parse_tree.children_[0]->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse node, service_op is not T_INT", K(parse_tree.children_[0]->type_));
  } else if (OB_FAIL(Util::resolve_relation_name(parse_tree.children_[1], service_name_str))) {
    LOG_WARN("fail to resolve service_name_str", KR(ret));
  } else {
    ObServiceNameArg::ObServiceOp service_op =
        static_cast<ObServiceNameArg::ObServiceOp>(parse_tree.children_[0]->value_);
    if (OB_FAIL(stmt->get_arg().init(service_op, target_tenant_id, service_name_str))) {
      LOG_WARN("fail to init ObServiceNameArg", KR(ret), K(service_op), K(target_tenant_id), K(service_name_str));
    }
  }
  if (OB_SUCC(ret)) {
    stmt_ = stmt;
  }
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    if (OB_ISNULL(schema_checker_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(schema_checker_->check_ora_ddl_priv(
      session_info_->get_effective_tenant_id(),
      session_info_->get_priv_user_id(),
      ObString(""),
      // why use T_ALTER_SYSTEM_SET_PARAMETER?
      // because T_ALTER_SYSTEM_SET_PARAMETER has following traits:
      // T_ALTER_SYSTEM_SET_PARAMETER can allow dba to do an operation
      // and prohibit other user to do this operation
      // so we reuse this.
      stmt::T_ALTER_SYSTEM_SET_PARAMETER,
      session_info_->get_enable_role_array()))) {
      LOG_WARN("failed to check privilege", K(session_info_->get_effective_tenant_id()), K(session_info_->get_user_id()));
    }
  }
  return ret;
}
} // sql
} // oceanbase