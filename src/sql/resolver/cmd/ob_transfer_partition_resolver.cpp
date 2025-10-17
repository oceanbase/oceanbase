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

#include "sql/resolver/cmd/ob_transfer_partition_resolver.h"

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
int ObTransferPartitionResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (T_TRANSFER_PARTITION == parse_tree.type_) {
    if (OB_FAIL(resolve_transfer_partition_(parse_tree))) {
      LOG_WARN("failed to reslove transfer partition", KR(ret));
    }
  } else if (T_CANCEL_TRANSFER_PARTITION == parse_tree.type_) {
    if (OB_FAIL(resolve_cancel_transfer_partition_(parse_tree))) {
      LOG_WARN("failed to resolve cancel transfer partition", KR(ret));
    }
  } else if (T_BALANCE_JOB_OP == parse_tree.type_) {
    if (OB_FAIL(resolve_balance_job_op_(parse_tree))) {
      LOG_WARN("failed to resolve cancel balance job", KR(ret));
    }
  } else  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION", KR(ret), "type",
        get_type_name(parse_tree.type_));
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

int ObTransferPartitionResolver::resolve_transfer_partition_(const ParseNode &parse_tree)
{
int ret = OB_SUCCESS;
  ObTransferPartitionStmt *stmt = create_stmt<ObTransferPartitionStmt>();
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(T_TRANSFER_PARTITION != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION", KR(ret), "type",
        get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (2 != parse_tree.num_child_ || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree or session info", KR(ret), "num_child", parse_tree.num_child_,
        KP(session_info_));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse node is null", KR(ret),  KP(parse_tree.children_[0]));
  } else if (OB_FAIL(Util::get_and_verify_tenant_name(
      parse_tree.children_[1],
      false, /* allow_sys_meta_tenant */
      session_info_->get_effective_tenant_id(),
      target_tenant_id, "Transfer partition"))) {
    LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
        K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[1]));
  } else {
    ParseNode *transfer_partition_node = parse_tree.children_[0];
    switch(transfer_partition_node->type_) {
      case T_TRANSFER_PARTITION_TO_LS:
        if (OB_FAIL(resolve_transfer_partition_to_ls_(
            *transfer_partition_node,
            target_tenant_id,
            session_info_->get_effective_tenant_id(),
            stmt))) {
          LOG_WARN("fail to resolve transfer_partition_to_ls", KR(ret), K(target_tenant_id), K(session_info_->get_effective_tenant_id()));
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid transfer partition node type", KR(ret), "type", get_type_name(transfer_partition_node->type_));
    }
    if (OB_SUCC(ret)) {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObTransferPartitionResolver::resolve_cancel_transfer_partition_(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObTransferPartitionStmt *stmt = create_stmt<ObTransferPartitionStmt>();
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(T_CANCEL_TRANSFER_PARTITION != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION", KR(ret), "type",
        get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (2 != parse_tree.num_child_ || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree or session info", KR(ret), "num_child", parse_tree.num_child_,
        KP(session_info_));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse node is null", KR(ret),  KP(parse_tree.children_[0]));
  } else if (OB_FAIL(Util::get_and_verify_tenant_name(
      parse_tree.children_[1],
      false, /* allow_sys_meta_tenant */
      session_info_->get_effective_tenant_id(),
      target_tenant_id, "Cancel transfer partition"))) {
    LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
        K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[1]));
  } else {
    ParseNode *transfer_partition_node = parse_tree.children_[0];
    uint64_t table_id = OB_INVALID_ID;
    ObObjectID object_id = OB_INVALID_OBJECT_ID;
    rootserver::ObTransferPartitionArg::ObTransferPartitionType type = rootserver::ObTransferPartitionArg::INVALID_TYPE;
    if (T_PARTITION_INFO == transfer_partition_node->type_) {
      type = rootserver::ObTransferPartitionArg::CANCEL_TRANSFER_PARTITION;
      if (OB_FAIL(resolve_part_info_(*transfer_partition_node, table_id, object_id))) {
        LOG_WARN("failed to resolve part info", KR(ret));
      }
    } else if (T_ALL == transfer_partition_node->type_) {
      type = rootserver::ObTransferPartitionArg::CANCEL_TRANSFER_PARTITION_ALL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid transfer partition node type", KR(ret), "type", get_type_name(transfer_partition_node->type_));
    }
    if (FAILEDx(stmt->get_arg().init_for_cancel_transfer_partition(
            target_tenant_id, type, table_id, object_id))) {
      LOG_WARN("fail to init stmt rpc arg", KR(ret), K(target_tenant_id), K(type),
          K(table_id), K(object_id));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObTransferPartitionResolver::resolve_balance_job_op_(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObTransferPartitionStmt *stmt = create_stmt<ObTransferPartitionStmt>();
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  rootserver::ObTransferPartitionArg::ObTransferPartitionType balance_job_type;
  if (OB_UNLIKELY(T_BALANCE_JOB_OP != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_BALANCE_JOB_OP", KR(ret), "type",
        get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (2 != parse_tree.num_child_
        || OB_ISNULL(session_info_)
        || OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree or session info", KR(ret), "num_child", parse_tree.num_child_,
        KP(session_info_));
  } else if (OB_FAIL(Util::get_and_verify_tenant_name(
      parse_tree.children_[1],
      false, /* allow_sys_meta_tenant */
      session_info_->get_effective_tenant_id(),
      target_tenant_id, "Cancel balance job"))) {
    LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
        K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[0]));
  } else if (OB_ISNULL(parse_tree.children_[0])
      || OB_UNLIKELY(T_INT != parse_tree.children_[0]->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("balance_job_op is invalid", KR(ret), KP(parse_tree.children_[0]));
  } else if (1 == parse_tree.children_[0]->value_) {
    balance_job_type = rootserver::ObTransferPartitionArg::CANCEL_BALANCE_JOB;
  } else if (2 == parse_tree.children_[0]->value_) {
    balance_job_type = rootserver::ObTransferPartitionArg::SUSPEND_BALANCE_JOB;
  } else if (3 == parse_tree.children_[0]->value_) {
    balance_job_type = rootserver::ObTransferPartitionArg::RESUME_BALANCE_JOB;
  }
  if (FAILEDx(stmt->get_arg().init_for_balance_job_op(
            target_tenant_id, balance_job_type))) {
      LOG_WARN("fail to init stmt rpc arg", KR(ret), K(target_tenant_id), K(balance_job_type));
  } else {
    stmt_ = stmt;
  }
  return ret;
}

int ObTransferPartitionResolver::resolve_part_info_(
    const ParseNode &parse_node,
    uint64_t &table_id,
    ObObjectID &object_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  object_id = OB_INVALID_OBJECT_ID;
  if (OB_UNLIKELY(T_PARTITION_INFO != parse_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_PARTITION_INFO", KR(ret), "type",
        get_type_name(parse_node.type_));
  } else if (2 != parse_node.num_child_
        || OB_ISNULL(parse_node.children_[0])
        || OB_ISNULL(parse_node.children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse node", KR(ret), "num_child", parse_node.num_child_,
        KP(parse_node.children_[0]), KP(parse_node.children_[1]));
  } else {
    table_id = parse_node.children_[0]->value_;
    object_id = parse_node.children_[1]->value_;
  }
  return ret;
}
int ObTransferPartitionResolver::resolve_transfer_partition_to_ls_(
    const ParseNode &parse_node,
    const uint64_t target_tenant_id,
    const uint64_t exec_tenant_id,
    ObTransferPartitionStmt *stmt)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  ObObjectID object_id = OB_INVALID_OBJECT_ID;
  if (OB_UNLIKELY(T_TRANSFER_PARTITION_TO_LS != parse_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION_TO_LS", KR(ret), "type",
        get_type_name(parse_node.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", KR(ret), KP(stmt));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(exec_tenant_id) || !is_valid_tenant_id(target_tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid exec_tenant_id or target_tenant_id", KR(ret), K(exec_tenant_id), K(target_tenant_id));
  } else if (2 != parse_node.num_child_
        || OB_ISNULL(parse_node.children_[0])
        || OB_ISNULL(parse_node.children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse node", KR(ret), "num_child", parse_node.num_child_,
        KP(parse_node.children_[0]), KP(parse_node.children_[1]));
  } else if (OB_FAIL(resolve_part_info_(*parse_node.children_[0], table_id, object_id))) {
    LOG_WARN("fail to resolve partition info", KR(ret), KP(parse_node.children_[0]));
  } else {
    int64_t id = parse_node.children_[1]->value_;
    ObLSID ls_id(id);
    if (OB_FAIL(stmt->get_arg().init_for_transfer_partition_to_ls(
        target_tenant_id,
        table_id,
        object_id,
        ls_id))) {
      LOG_WARN("fail to init stmt rpc arg", KR(ret), K(target_tenant_id),
          K(table_id), K(object_id), K(ls_id));
    }
  }
  return ret;
}
} // sql
} // oceanbase
