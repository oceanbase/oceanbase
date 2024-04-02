/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX RS

#include "ob_transfer_partition_command.h"

#include "lib/oblog/ob_log_module.h"                 // LOG_*
#include "share/ob_balance_define.h"                 // need_balance_table()
#include "share/balance/ob_transfer_partition_task_table_operator.h"
#include "share/ob_tenant_info_proxy.h"
#include "rootserver/standby/ob_standby_service.h"
#include "observer/omt/ob_tenant_config_mgr.h" // ObTenantConfigGuard


namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;
using namespace obrpc;
namespace rootserver
{
int ObTransferPartitionArg::init_for_transfer_partition_to_ls(
      const uint64_t target_tenant_id,
      const uint64_t table_id,
      const ObObjectID object_id,
      const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(target_tenant_id)
      && OB_INVALID_ID != table_id
      && OB_INVALID_OBJECT_ID != object_id
      && !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_tenant_id),
        K(table_id), K(object_id), K(ls_id));
  } else {
    type_ = TRANSFER_PARTITION_TO_LS;
    target_tenant_id_ = target_tenant_id;
    table_id_ = table_id;
    object_id_ = object_id;
    ls_id_ = ls_id;
  }
  return ret;
}
bool ObTransferPartitionArg::is_valid() const
{
  int bret = false;
  if (OB_UNLIKELY(type_ <= INVALID_TYPE || type_ >= MAX_TYPE)) {
    bret = false;
  } else if (TRANSFER_PARTITION_TO_LS == type_
      && is_valid_tenant_id(target_tenant_id_)
      && OB_INVALID_ID != table_id_
      && OB_INVALID_OBJECT_ID != object_id_
      && ls_id_.is_valid()) {
    bret = true;
  }
  return bret;
}

int ObTransferPartitionArg::assign(const ObTransferPartitionArg &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    type_ = other.type_;
    target_tenant_id_ = other.target_tenant_id_;
    table_id_ = other.table_id_;
    object_id_ = other.object_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}
int ObTransferPartitionCommand::execute(const ObTransferPartitionArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.get_target_tenant_id();
  const ObLSID &ls_id = arg.get_ls_id();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(check_tenant_status_data_version_and_config_(tenant_id))) {
    LOG_WARN("fail to check tenant", KR(ret), K(arg));
  } else if (OB_FAIL(check_table_schema_(tenant_id, arg.get_table_id(), arg.get_object_id()))) {
    LOG_WARN("fail to execute check_table_schema_", KR(ret), K(arg));
  } else if (OB_FAIL(check_ls_(tenant_id, ls_id))) {
    LOG_WARN("fail to execute check_ls_", KR(ret), K(tenant_id), K(ls_id));
  } else {
    ObTransferPartInfo part_info(arg.get_table_id(), arg.get_object_id());
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObTransferPartitionTaskTableOperator::insert_new_task(
        tenant_id,
        part_info,
        ls_id,
        trans))) {
      LOG_WARN("fail to insert new task", KR(ret), K(tenant_id), K(part_info), K(ls_id));
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        ret = OB_ENTRY_EXIST;
        LOG_USER_ERROR(OB_ENTRY_EXIST, "Partition task already exists");
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(!trans.is_started())) {
      LOG_WARN("the transaction is not started");
    } else {
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit the transaction", KR(ret), KR(tmp_ret), K(tenant_id), K(part_info), K(ls_id));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObTransferPartitionCommand::check_tenant_status_data_version_and_config_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  ObAllTenantInfo tenant_info;
  ObTenantStatus tenant_status = TENANT_STATUS_MAX;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  bool bret = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(OB_STANDBY_SERVICE.get_tenant_status(tenant_id, tenant_status))) {
    LOG_WARN("fail to get tenant status", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_tenant_normal(tenant_status))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's status is not normal", KR(ret), K(tenant_status));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant status is not NORMAL, transfer partition is");
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
  } else if (!(bret = tenant_config->enable_transfer)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("enable_transfer is off", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Transfer is disabled, transfer partition is");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_2_1_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Tenant COMPATIBLE is below 4.2.1.2, transfer partition is not supported", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Tenant COMPATIBLE is below 4.2.1.2, transfer partition is");
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
      tenant_id, GCTX.sql_proxy_, false /*for_update*/, tenant_info))) {
    LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!tenant_info.is_primary())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's role is not primary", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant role is not PRIMARY, transfer partition is");
  } else if (OB_UNLIKELY(!tenant_info.is_normal_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's switchover status is not normal", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant switchover status is not NORMAL, transfer partition is");
  }
  return ret;
}

int ObTransferPartitionCommand::check_table_schema_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObObjectID &object_id)
{
  int ret = OB_SUCCESS;
  const uint64_t allocator_tenant_id = is_valid_tenant_id(MTL_ID()) ? MTL_ID() : OB_SYS_TENANT_ID;
  common::ObArenaAllocator tmp_allocator("TABLE_SCHEMA", OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_tenant_id);
  ObSimpleTableSchemaV2* table_schema = NULL;
  ObTabletID tablet_id;
  const char* table_type_str = NULL;
  const int64_t ERR_MSG_BUF_LEN = 255;
  char table_type_err_msg[ERR_MSG_BUF_LEN] = "";
  int64_t pos = 0;
  bool need_balance = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObSchemaUtils::get_latest_table_schema(
      *GCTX.sql_proxy_,
      tmp_allocator,
      tenant_id,
      table_id,
      table_schema))) {
    LOG_WARN("fail to get latest table schema", KR(ret), K(tenant_id), K(table_id),
        K(allocator_tenant_id), K(MTL_ID()));
    if (OB_TABLE_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "Table not exists");
    }
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", KR(ret), KP(table_schema));
  } else if (FALSE_IT(need_balance = check_if_need_balance_table(*table_schema, table_type_str))) {
    //can not be here
  } else if (!need_balance) {
    ret = OB_OP_NOT_ALLOW;
    int tmp_ret = OB_SUCCESS;
    LOG_WARN("only get related tables for user table and other need balance table", KR(ret),
        K(table_id), K(table_schema));
    if (OB_TMP_FAIL(databuff_printf(
        table_type_err_msg,
        ERR_MSG_BUF_LEN,
        pos,
        "Transfer partition of '%s' is",
        table_type_str))) {
      LOG_WARN("fail to execute databuff_printf", KR(ret), KR(tmp_ret), K(table_type_str));
    } else {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, table_type_err_msg);
    }
  } else if (OB_FAIL(table_schema->get_tablet_id_by_object_id(object_id, tablet_id))) {
    LOG_WARN("fail to get tablet id", KR(ret));
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "Partition not exists");
    }
  }
  return ret;
}

int ObTransferPartitionCommand::check_ls_(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!ls_id.is_user_ls())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("transfer partition to sys ls is not allowed", KR(ret), K(ls_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Transfer partition to SYS LS is");
  } else {
    share::ObLSAttrOperator ls_operator(tenant_id, GCTX.sql_proxy_);
    ObLSAttr ls_attr;
    if (OB_FAIL(ls_operator.get_ls_attr(ls_id, false /*for_update*/, *GCTX.sql_proxy_, ls_attr))) {
      LOG_WARN("fail to get ls attr", KR(ret), K(tenant_id), K(ls_id));
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "LS not exists");
      }
    } else if (OB_UNLIKELY(!ls_attr.ls_is_normal())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ls status is not normal", KR(ret), K(ls_attr));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "LS status is not NORMAL, transfer partition is");
    } else if (OB_UNLIKELY(ls_attr.get_ls_flag().is_block_tablet_in())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ls flag is block_tablet_in", KR(ret), K(ls_attr));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "LS is in BLOCK_TABLET_IN state, transfer partition is");
    } else if (OB_UNLIKELY(ls_attr.get_ls_flag().is_duplicate_ls())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("transfer partition to duplicate ls is not allowed", KR(ret), K(ls_attr));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Transfer partition to DUPLICATE LS is");
    }
  }
  return ret;
}

}
}
