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

#include "sql/resolver/cmd/ob_alter_ls_resolver.h"

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
int ObAlterLSResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAlterLSStmt *stmt = create_stmt<ObAlterLSStmt>();
  if (OB_UNLIKELY(T_ALTER_LS !=  parse_tree.type_
        || 1 >= parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not ALTER_LS", KR(ret),
        "type", get_type_name(parse_tree.type_), "child num", parse_tree.num_child_);
  } else {
    ParseNode *op_node = parse_tree.children_[0];
    if (OB_ISNULL(op_node) || T_INT != op_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op node is null", KR(ret), KP(op_node));
    } else if (2 == op_node->value_) {
      if (OB_FAIL(resolve_modify_ls_(parse_tree, stmt))) {
        LOG_WARN("failed to resolve modify ls", KR(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid parse value", KR(ret), "value",  parse_tree.value_);
    }
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

int ObAlterLSResolver::resolve_modify_ls_(const ParseNode &parse_tree, ObAlterLSStmt *stmt)
{
  int ret = OB_SUCCESS;
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  uint64_t ug_id = OB_INVALID_ID;
  ObZone primary_zone;
  if (OB_ISNULL(stmt) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or session_info_ is null", KR(ret), KP(stmt), KP(session_info_));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_11) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("CLUSTER_VERSION < 4.2.1.11", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "CLUSTER_VERSION < 4.2.1.11, MODIFY LS is");
  } else {
    if (4 != parse_tree.num_child_ || OB_ISNULL(parse_tree.children_[1])
        || OB_ISNULL(parse_tree.children_[2]) || OB_ISNULL(parse_tree.children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid parse tree or session info", KR(ret),
          "num_child", parse_tree.num_child_);
    } else if (2 != parse_tree.children_[0]->value_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("operation type is invalid", KR(ret),
          "value", parse_tree.children_[1]->value_);
    } else if (OB_FAIL(resolve_ls_attr_(*parse_tree.children_[2], ug_id, primary_zone))) {
      LOG_WARN("failed to resolve ls attr", KR(ret));
    } else if (OB_FAIL(Util::get_and_verify_tenant_name(parse_tree.children_[3], true, /* allow_sys_meta_tenant */
            session_info_->get_effective_tenant_id(), target_tenant_id, "Modify LS"))) {
      LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
          K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[3]));
    } else if (T_LS != parse_tree.children_[1]->type_ || 1 != parse_tree.children_[1]->num_child_
        || OB_ISNULL(parse_tree.children_[1]->children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls tree is invalid", KR(ret), "num_child", parse_tree.children_[1]->num_child_,
          "type", parse_tree.children_[1]->type_);
    } else if (T_INT != parse_tree.children_[1]->children_[0]->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("parse node type is invalid", KR(ret), "type", parse_tree.children_[1]->children_[0]->type_);
    } else {
      ObLSID id(parse_tree.children_[1]->children_[0]->value_);
      if (OB_FAIL(stmt->get_arg().init_modify_ls(target_tenant_id, id, ug_id, primary_zone))) {
        LOG_WARN("failed to init modify ls", KR(ret), K(target_tenant_id), K(id), K(ug_id), K(primary_zone));
      }
    }
  }
  return ret;
}

int ObAlterLSResolver::resolve_ls_attr_(const ParseNode &parse_tree,
    uint64_t &unit_group_id, ObZone &primary_zone)
{
  int ret = OB_SUCCESS;
  unit_group_id = OB_INVALID_ID;
  primary_zone.reset();
  if (OB_UNLIKELY(T_LS_ATTR_LIST != parse_tree.type_
        || 1 > parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node", KR(ret), "type", get_type_name(parse_tree.type_),
        "num", parse_tree.num_child_);
  } else {
    int64_t num = parse_tree.num_child_;
    for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      ParseNode *node = parse_tree.children_[i];
      if (OB_ISNULL(node) || 1 != node->num_child_
          || OB_ISNULL(node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", KR(ret), K(i), K(num), KP(node));
      } else {
        ParseNode *attr_node = node->children_[0];
        if (T_UNIT_GROUP == node->type_) {
          unit_group_id = attr_node->value_;
          if (OB_USER_UNIT_GROUP_ID >= unit_group_id && 0 != unit_group_id) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("set unit group id invalid not allowed", KR(ret), K(unit_group_id));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set invalid unit group");
          }
        } else if (T_PRIMARY_ZONE == node->type_) {
          common::ObString primary_zone_str;
          primary_zone_str.assign_ptr(const_cast<char *>(attr_node->str_value_),
                                  static_cast<int32_t>(attr_node->str_len_));
          if (primary_zone_str.empty()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("set primary_zone empty is not allowed now", KR(ret));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set primary_zone empty");
          } else if (OB_FAIL(primary_zone.assign(primary_zone_str))) {
            LOG_WARN("failed to assign primary zone", KR(ret), K(primary_zone_str));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown node type", KR(ret), K(node->type_));
        }
      }
    }//end for
  }
  return ret;
}

} // sql
} // oceanbase