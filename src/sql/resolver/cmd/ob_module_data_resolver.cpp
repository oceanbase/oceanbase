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
#include "sql/resolver/cmd/ob_module_data_resolver.h"
#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"
#include "sql/resolver/cmd/ob_alter_system_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace table;
namespace sql
{
int ObModuleDataResolver::resolve_module(const ParseNode *node, table::ObModuleDataArg::ObExecModule &mod)
{
  int ret = OB_SUCCESS;
  ObString module_name;
  if (OB_ISNULL(node) || OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret), KP(node));
  } else if (node->type_ != T_MODULE_NAME) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_MODULE_NAME", "type", get_type_name(node->type_));
  } else {
    node = node->children_[0];
    if (node->str_len_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty module name");
    } else {
      module_name = ObString(node->str_len_, node->str_value_);
      if (module_name.case_compare("redis") == 0) {
        mod = ObModuleDataArg::REDIS;
      } else if (module_name.case_compare("gis") == 0) {
        mod = ObModuleDataArg::GIS;
      } else if (module_name.case_compare("timezone") == 0) {
        mod = ObModuleDataArg::TIMEZONE;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid module str", K(ret), K(module_name));
      }
    }
  }
  return ret;
}

int ObModuleDataResolver::resolve_target_tenant_id(const ParseNode *node, uint64_t &target_tenant_id)
{
  int ret = OB_SUCCESS;
  const uint64_t login_tenant_id = session_info_->get_login_tenant_id();
  target_tenant_id = OB_INVALID_ID;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name;
  ObSchemaGetterGuard schema_guard;
  ObString tenant_name_str;
  lib::Worker::CompatMode mode;
  uint64_t compat_version = 0;
  if (login_tenant_id != OB_SYS_TENANT_ID) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation from regular user tenant");
    LOG_WARN("non-sys tenant change tenant not allowed",
      K(ret), K(target_tenant_id), K(login_tenant_id));
  } else if (OB_FAIL(ObAlterSystemResolverUtil::resolve_tenant(node, tenant_name))) {
    LOG_WARN("resolve tenant_id failed", K(ret));
  } else if (FALSE_IT(tenant_name_str.assign(tenant_name.ptr(), tenant_name.size()))) {
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get_tenant_schema_guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name_str, target_tenant_id))) {
    LOG_WARN("failed to get tenant id from schema guard", KR(ret), K(tenant_name_str));
  } else if (OB_FAIL(is_meta_tenant(target_tenant_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation from meta tenant");
    LOG_WARN("operation from meta tenant not allowed", K(ret), K(target_tenant_id), K(login_tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(target_tenant_id, mode))) {
    LOG_WARN("fail to get tenant mode", K(ret), K(target_tenant_id));
  } else if (lib::Worker::CompatMode::ORACLE == mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation from oracle tenant");
    LOG_WARN("operation from oracle tenant not allowed", K(ret), K(target_tenant_id), K(login_tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(target_tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(target_tenant_id));
  } else if (compat_version < MOCK_DATA_VERSION_4_2_5_0
        || (DATA_VERSION_4_3_0_0 <= compat_version && compat_version < DATA_VERSION_4_3_5_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "target tenant versions lower than 4.2.5.0 or between 4.3.0.0 and 4.3.5.1");
    LOG_WARN("target tenant versions lower than 4.2.5.0 or between 4.3.0.0 and 4.3.5.1 not supported",
      K(ret), K(target_tenant_id), K(compat_version));
  }
  return ret;
}

int ObModuleDataResolver::resolve_file_path(const ParseNode *node, ObString &abs_path)
{
  int ret = OB_SUCCESS;
  // reference ObLoadDataResolver::resolve_filename
  if (OB_ISNULL(node)) {
    abs_path.reset();
  } else {
    // TODO: implement file path resolver in gis or timezone
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("load/check module data with 'infile' param", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "load/check module data with 'infile' param");
  }
  return ret;
}

int ObModuleDataResolver::resolve_exec_type(const ParseNode *node, table::ObModuleDataArg::ObInfoOpType &type)
{
  int ret = OB_SUCCESS;
  if (node->value_ <= table::ObModuleDataArg::INVALID_OP || node->value_ >= table::ObModuleDataArg::MAX_OP) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exec type", K(ret), K(node->value_));
  } else {
    type = static_cast<table::ObModuleDataArg::ObInfoOpType>(node->value_);
  }
  return ret;
}

int ObModuleDataResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObModuleDataStmt *stmt = create_stmt<ObModuleDataStmt>();
  if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObModuleDataStmt failed");
  } else if (OB_UNLIKELY(T_MODULE_DATA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_MODULE_DATA", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_) ||
            parse_tree.num_child_ != CHILD_NUM) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse node", K(ret), K(parse_tree.num_child_), KP(parse_tree.children_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < MOCK_CLUSTER_VERSION_4_2_5_0
      || (CLUSTER_VERSION_4_3_0_0 <= GET_MIN_CLUSTER_VERSION() && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("module_data is not supported under cluster version 4.2.5.0 or between 4.3.0.0 and 4.3.5.1", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "module_data in cluster under version 4.2.5.0 or between 4.3.0.0 and 4.3.5.1");
  } else {
    table::ObModuleDataArg &arg = stmt->get_arg();
    uint64_t compat_version = 0;
    if (OB_FAIL(resolve_exec_type(parse_tree.children_[TYPE_IDX], arg.op_))) {
      LOG_WARN("fail to resolve exec type", K(ret));
    } else if (OB_FAIL(resolve_module(parse_tree.children_[MODULE_IDX], arg.module_))) {
      LOG_WARN("fail to resolve module", K(ret));
    } else if (OB_FAIL(resolve_target_tenant_id(parse_tree.children_[TENANT_IDX], arg.target_tenant_id_))) {
      LOG_WARN("fail to resolve target tenant id", K(ret));
    } else if (OB_FAIL(resolve_file_path(parse_tree.children_[FILE_IDX], arg.file_path_))) {
      LOG_WARN("fail to resolve file path", K(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(arg.target_tenant_id_, compat_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(arg));
    } else if (compat_version < MOCK_DATA_VERSION_4_2_5_0
        || (DATA_VERSION_4_3_0_0 <= compat_version && compat_version < DATA_VERSION_4_3_5_1)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("module_data is not supported with tenant under version 4.2.5.0 or between 4.3.0.0 and 4.3.5.1", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "module_data with tenant under version 4.2.5.0 or between 4.3.0.0 and 4.3.5.1");
    } else if (!arg.file_path_.empty() && arg.module_ == ObModuleDataArg::REDIS) {
      ret = OB_PARTIAL_FAILED;
      LOG_USER_ERROR(OB_PARTIAL_FAILED, "loading redis module does not need to specify infile");
      LOG_WARN("loading redis module does not need to specify infile", K(ret), K(arg.file_path_));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase