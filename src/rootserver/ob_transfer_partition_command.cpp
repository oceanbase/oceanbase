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
#include "share/balance/ob_balance_job_table_operator.h"//ObBalanceJobTableOperator
#include "share/transfer/ob_transfer_task_operator.h"//ObTransferTask
#include "share/ob_tenant_info_proxy.h"
#include "share/ob_primary_standby_service.h"
#include "observer/omt/ob_tenant_config_mgr.h" // ObTenantConfigGuard
#include "share/ls/ob_ls_i_life_manager.h"//START/END_TRANSACTION
#include "storage/tablelock/ob_lock_utils.h"//table_lock


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
      const ObObjectID &object_id,
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

int ObTransferPartitionArg::init_for_cancel_transfer_partition(
      const uint64_t target_tenant_id,
      const ObTransferPartitionType type,
      const uint64_t table_id,
      const ObObjectID &object_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(target_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_tenant_id),
        K(table_id), K(object_id));
  } else if (CANCEL_TRANSFER_PARTITION == type) {
    if (OB_UNLIKELY(OB_INVALID_ID == table_id
            || OB_INVALID_OBJECT_ID == object_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(table_id), K(object_id));
    } else {
      table_id_ = table_id;
      object_id_ = object_id;
    }
  } else if (CANCEL_TRANSFER_PARTITION_ALL == type) {
    //nothing to check
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is invalid", KR(ret), K(type));
  }

  if (OB_SUCC(ret)) {
    type_ = type;
    target_tenant_id_ = target_tenant_id;
  }
  return ret;

}

int ObTransferPartitionArg::init_for_cancel_balance_job(
      const uint64_t target_tenant_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(target_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_tenant_id));
  } else {
    type_ = CANCEL_BALANCE_JOB;
    target_tenant_id_ = target_tenant_id;
  }
  return ret;
}

bool ObTransferPartitionArg::is_valid() const
{
  int bret = false;
  if (OB_UNLIKELY(type_ <= INVALID_TYPE || type_ >= MAX_TYPE)) {
    bret = false;
  } else if (!is_valid_tenant_id(target_tenant_id_)) {
    bret = false;
  } else if (TRANSFER_PARTITION_TO_LS == type_
      && OB_INVALID_ID != table_id_
      && OB_INVALID_OBJECT_ID != object_id_
      && ls_id_.is_valid()) {
    bret = true;
  } else if (CANCEL_TRANSFER_PARTITION == type_
      && OB_INVALID_ID != table_id_
      && OB_INVALID_OBJECT_ID != object_id_) {
     bret = true;
  } else if (CANCEL_TRANSFER_PARTITION_ALL == type_
      || CANCEL_BALANCE_JOB == type_) {
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
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (arg.is_transfer_partition_to_ls()) {
    if (OB_FAIL(execute_transfer_partition_(arg))) {
      LOG_WARN("failed to execute transfer partition", KR(ret), K(arg));
    }
  } else if (arg.is_cancel_transfer_partition()) {
    if (OB_FAIL(execute_cancel_transfer_partition_(arg))) {
      LOG_WARN("failed to execute cancel transfer partition", KR(ret), K(arg));
    }
  } else if (arg.is_cancel_transfer_partition_all()) {
    if (OB_FAIL(execute_cancel_transfer_partition_all_(arg))) {
      LOG_WARN("failed to execute cancel transfer partition all", KR(ret), K(arg));
    }
  } else if (arg.is_cancel_balance_job()) {
    if (OB_FAIL(execute_cancel_balance_job_(arg))) {
      LOG_WARN("failed to execute cancel balance job", KR(ret), K(arg));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is unexpected", KR(ret), K(arg));
  }
  return ret;
}

int ObTransferPartitionCommand::execute_transfer_partition_(const ObTransferPartitionArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.get_target_tenant_id();
  const ObLSID &ls_id = arg.get_ls_id();
  if (OB_UNLIKELY(!arg.is_valid() || !arg.is_transfer_partition_to_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(check_tenant_status_(tenant_id))) {
    LOG_WARN("failed to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_data_version_and_config_(tenant_id))) {
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

int ObTransferPartitionCommand::check_data_version_and_config_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  uint64_t compat_version = 0;
  bool bret = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
  } else if (!(bret = tenant_config->enable_transfer)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("enable_transfer is off", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Transfer is disabled, transfer partition is");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_2_1_2
      //由于transfer partition在4220版本上存在，所以不用判断4220之后的42x分支
      || (compat_version >= DATA_VERSION_4_3_0_0 && compat_version < DATA_VERSION_4_3_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Tenant COMPATIBLE is below target version, transfer partition is not supported", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Tenant COMPATIBLE is below target version, transfer partition is");
  }
  return ret;
}

int ObTransferPartitionCommand::check_tenant_status_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  ObTenantStatus tenant_status = TENANT_STATUS_MAX;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.get_tenant_status(tenant_id, tenant_status))) {
    LOG_WARN("fail to get tenant status", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_tenant_normal(tenant_status))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's status is not normal", KR(ret), K(tenant_status));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant status is not NORMAL. Operation is");
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
      tenant_id, GCTX.sql_proxy_, false /*for_update*/, tenant_info))) {
    LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!tenant_info.is_primary())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's role is not primary", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant role is not PRIMARY. Operation is");
  } else if (OB_UNLIKELY(!tenant_info.is_normal_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's switchover status is not normal", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant switchover status is not NORMAL. Operation is");
  }
  return ret;
}

int ObTransferPartitionCommand::check_data_version_for_cancel_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to check data version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < MOCK_DATA_VERSION_4_2_1_4
      || (compat_version >= DATA_VERSION_4_2_2_0 && compat_version < MOCK_DATA_VERSION_4_2_4_0)
      || (compat_version >= DATA_VERSION_4_3_0_0 && compat_version < DATA_VERSION_4_3_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Tenant COMPATIBLE is below target version. Operation is");
    LOG_WARN("Tenant COMPATIBLE is below target version, command is not supported",
        KR(ret), K(compat_version));
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
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "LS status is not NORMAL. Operation is");
    } else if (OB_UNLIKELY(ls_attr.get_ls_flag().is_block_tablet_in())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ls flag is block_tablet_in", KR(ret), K(ls_attr));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "LS is in BLOCK_TABLET_IN state. Operation is");
    } else if (OB_UNLIKELY(ls_attr.get_ls_flag().is_duplicate_ls())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("transfer partition to duplicate ls is not allowed", KR(ret), K(ls_attr));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Transfer partition to DUPLICATE LS is");
    }
  }
  return ret;
}

int ObTransferPartitionCommand::execute_cancel_transfer_partition_(const ObTransferPartitionArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.get_target_tenant_id();
  if (OB_UNLIKELY(!arg.is_valid() || !arg.is_cancel_transfer_partition())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(check_tenant_status_(tenant_id))) {
    LOG_WARN("failed to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_data_version_for_cancel_(tenant_id))) {
    LOG_WARN("failed to check data version for cancel", KR(ret), K(tenant_id));
  } else {
    ObTransferPartitionTask task;
    START_TRANSACTION(GCTX.sql_proxy_, tenant_id)
    if (FAILEDx(ObInnerTableLockUtil::lock_inner_table_in_trans(trans,
            tenant_id, OB_ALL_BALANCE_JOB_TID, EXCLUSIVE, true))) {
      LOG_WARN("lock inner table failed", KR(ret), K(arg));
    } else if (OB_FAIL(ObTransferPartitionTaskTableOperator::get_transfer_partition_task(
            tenant_id, ObTransferPartInfo(arg.get_table_id(), arg.get_object_id()),
            task, trans))) {
      LOG_WARN("failed to get transfer partition task", KR(ret), K(arg));
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "Transfer partition task not exist");
      }
    } else if (OB_FAIL(try_cancel_transfer_partition_(task, trans))) {
      LOG_WARN("failed to try cancel transfer partition", KR(ret), K(task));
    }
    END_TRANSACTION(trans);
  }
  return ret;
}

int ObTransferPartitionCommand::execute_cancel_transfer_partition_all_(const ObTransferPartitionArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.get_target_tenant_id();
  if (OB_UNLIKELY(!arg.is_valid() || !arg.is_cancel_transfer_partition_all())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (OB_FAIL(check_tenant_status_(tenant_id))) {
    LOG_WARN("failed to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_data_version_for_cancel_(tenant_id))) {
    LOG_WARN("failed to check data version for cancel", KR(ret), K(tenant_id));
  } else {
    ObTransferPartitionTaskStatus status(share::ObTransferPartitionTaskStatus::TRP_TASK_STATUS_CANCELED);
    ObTransferPartList wait_task;
    ObTransferPartList init_task;
    ObTransferPartList doing_task;
    ObTransferPartitionTaskID max_task_id;

    START_TRANSACTION(GCTX.sql_proxy_, tenant_id)
    if (FAILEDx(ObInnerTableLockUtil::lock_inner_table_in_trans(trans,
            tenant_id, OB_ALL_BALANCE_JOB_TID, EXCLUSIVE, true))) {
      LOG_WARN("lock inner table failed", KR(ret), K(arg));
    } else if (OB_FAIL(get_transfer_part_list_(tenant_id, wait_task, init_task,
            doing_task, max_task_id, trans))) {
      LOG_WARN("failed to get transfer part list", KR(ret), K(tenant_id));
    }
    if (OB_SUCC(ret) && wait_task.count() > 0) {
      if (OB_FAIL(ObTransferPartitionTaskTableOperator::finish_task(
              tenant_id, wait_task, max_task_id,
              status, "Canceled in WAITING status", trans))) {
        LOG_WARN("failed to finish task", KR(ret), K(tenant_id), K(wait_task), K(max_task_id));
      }
    }
    if (OB_SUCC(ret) && init_task.count() > 0) {
      //存在init状态的任务，不管有没有doing状态的任务，为了保证新创建出来的日志流都可以回收掉
      //都要挨个处理balance_task的part_list，不能简单的把balance_job给cancel掉。
      if (OB_FAIL(cancel_all_init_transfer_partition_(tenant_id, init_task, trans))) {
        LOG_WARN("failed to cancel all init transfer partition", KR(ret), K(tenant_id), K(init_task));
      } else if (OB_FAIL(ObTransferPartitionTaskTableOperator::finish_task(
              tenant_id, init_task, max_task_id, status,
              "Canceled in INIT status", trans))) {
        LOG_WARN("failed to finish task", KR(ret), K(tenant_id), K(init_task), K(max_task_id));
      }
    }
    END_TRANSACTION(trans);
    if (OB_SUCC(ret) && doing_task.count() > 0) {
      ret = OB_PARTIAL_FAILED;
      LOG_WARN("has partition is in transfer", K(doing_task));
      LOG_USER_ERROR(OB_PARTIAL_FAILED,
          "Some tasks are already in DOING status, can not be canceled");
    }
  }

  return ret;
}

int ObTransferPartitionCommand::get_transfer_part_list_(const uint64_t tenant_id,
    ObTransferPartList &wait_list, ObTransferPartList &init_list,
    ObTransferPartList &doing_list, ObTransferPartitionTaskID &max_task_id,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObTransferPartitionTask> task_array;
  wait_list.reset();
  init_list.reset();
  doing_list.reset();
  max_task_id.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTransferPartitionTaskTableOperator::load_all_task(
          tenant_id, task_array, trans))) {
    LOG_WARN("failed to get all transfer partition task", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
    const ObTransferPartitionTask &task = task_array.at(i);
    if (!max_task_id.is_valid() || task.get_task_id() > max_task_id) {
      max_task_id = task.get_task_id();
    }
    if (task.get_task_status().is_waiting()) {
      if (OB_FAIL(wait_list.push_back(task.get_part_info()))) {
        LOG_WARN("failed to push back", KR(ret), K(task));
      }
    } else if (task.get_task_status().is_init()) {
      if (OB_FAIL(init_list.push_back(task.get_part_info()))) {
        LOG_WARN("failed to push back", KR(ret), K(task));
      }
    } else if (task.get_task_status().is_doing()) {
      if (OB_FAIL(doing_list.push_back(task.get_part_info()))) {
        LOG_WARN("failed to push back", KR(ret), K(task));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task status is unexpected", KR(ret), K(task));
    }
  }

  return ret;
}

int ObTransferPartitionCommand::cancel_all_init_transfer_partition_(const uint64_t tenant_id,
      const share::ObTransferPartList &init_list,
      common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObBalanceTaskArray balance_tasks;
  ObBalanceJob balance_job;
  int64_t start_time = 0, finish_time = 0;//no used

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || 0 == init_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(init_list));
  } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(tenant_id,
            true, trans, balance_job, start_time, finish_time))) {
    LOG_WARN("failed to get balance job", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!balance_job.get_job_type().is_transfer_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("balance job must be transfer partition", KR(ret), K(balance_job));
  } else if (OB_FAIL(ObBalanceTaskTableOperator::load_task(tenant_id, balance_tasks, trans))) {
    LOG_WARN("failed to load task", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < balance_tasks.count(); ++i) {
    const ObBalanceTask &balance_task = balance_tasks.at(i);
    const ObTransferTaskID transfer_task_id = balance_task.get_current_transfer_task_id();
    ObTransferPartList new_part_list;
    new_part_list.reset();
    if (!transfer_task_id.is_valid()) {
      //如果没有发起过transfer，直接清空当前的part_list
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < balance_task.get_part_list().count(); ++j) {
        const ObTransferPartInfo &part = balance_task.get_part_list().at(j);
        if (!has_exist_in_array(init_list, part)) {
          //如果part不是init task的范围，则需要保留
          if (OB_FAIL(new_part_list.push_back(part))) {
            LOG_WARN("failed to push back", KR(ret), K(part), K(balance_task), K(new_part_list));
          }
        }
      }//end for j
      if (OB_SUCC(ret) && 0 == new_part_list.count()) {
        //如果存在current_transfer_task，则这个balance_task肯定有part处于doing状态
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("part list can not be empty", KR(ret), K(balance_task), K(init_list));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (balance_task.get_part_list().count() == new_part_list.count()) {
      //个数相等应该part_list就是相等的，不去做其他的校验了
      LOG_INFO("part list no change, no need update", K(balance_task), K(new_part_list), K(init_list));
    } else if (OB_FAIL(ObBalanceTaskTableOperator::update_task_part_list(tenant_id,
            balance_task.get_balance_task_id(), new_part_list, trans))) {
      LOG_WARN("failed to update task part list", KR(ret), K(tenant_id), K(balance_task),
          K(new_part_list));
    }
  }//end for

  return ret;
}


int ObTransferPartitionCommand::try_cancel_transfer_partition_(
      const share::ObTransferPartitionTask &task,
      common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTransferPartList part_list;
  ObTransferPartitionTaskStatus status(share::ObTransferPartitionTaskStatus::TRP_TASK_STATUS_CANCELED);
  ObSqlString comment;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task));
  } else if (OB_FAIL(part_list.push_back(task.get_part_info()))) {
    LOG_WARN("failed to push back part_info", KR(ret), K(task));
  } else if (OB_FAIL(comment.assign_fmt("Canceled in %s status",
          task.get_task_status().to_str()))) {
    LOG_WARN("failed to assign comment", KR(ret), K(task));
  } else if (task.get_task_status().is_waiting()) {
  } else if (task.get_task_status().is_init()) {
    //直接去修改balance_task的part_list
    if (OB_FAIL(cancel_transfer_partition_in_init_(
            task, trans))) {
      LOG_WARN("failed to cancel transfer partition", KR(ret), K(task));
    }
  } else if (task.get_task_status().is_doing()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("task is in transfer, can not cancel", KR(ret), K(task));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW,
        "Task is already in DOING status. Operation is");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is unexpected", KR(ret), K(task));
  }
  if (FAILEDx(ObTransferPartitionTaskTableOperator::finish_task(
          task.get_tenant_id(), part_list, task.get_task_id(),
          status, comment.string(), trans))) {
    LOG_WARN("failed to finish task", KR(ret), K(task), K(part_list));
  }
  return ret;
}

int ObTransferPartitionCommand::cancel_transfer_partition_in_init_(
    const share::ObTransferPartitionTask &task,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTransferPartList part_list;
  ObBalanceTaskArray balance_tasks;
  const uint64_t tenant_id = task.get_tenant_id();
  bool found = false;//不存在找不到balance_task的情况，也可能一个transfer_partition涉及多个task
  ObBalanceJob balance_job;
  int64_t start_time = 0, finish_time = 0;//no used

  if (OB_UNLIKELY(!task.is_valid() || !task.get_task_status().is_init())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task));
  } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(tenant_id,
            true, trans, balance_job, start_time, finish_time))) {
    LOG_WARN("failed to get balance job", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!balance_job.get_job_type().is_transfer_partition()
        || task.get_balance_job_id() != balance_job.get_job_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("balance job must be transfer partition", KR(ret), K(balance_job), K(task));
  } else if (OB_FAIL(ObBalanceTaskTableOperator::load_task(tenant_id, balance_tasks, trans))) {
    LOG_WARN("failed to load task", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < balance_tasks.count(); ++i) {
    const ObBalanceTask &balance_task = balance_tasks.at(i);
    int64_t idx = 0;
    if (has_exist_in_array(balance_task.get_part_list(), task.get_part_info(), &idx)) {
      found = true;
      ObTransferPartList new_part_list;
      if (OB_FAIL(new_part_list.assign(balance_task.get_part_list()))) {
        LOG_WARN("failed to assign part list", KR(ret), K(balance_task));
      } else if (OB_FAIL(new_part_list.remove(idx))) {
        LOG_WARN("failed to remove idx", KR(ret), K(idx), K(balance_task), K(new_part_list));
      } else if (OB_FAIL(ObBalanceTaskTableOperator::update_task_part_list(
              tenant_id, balance_task.get_balance_task_id(), new_part_list, trans))) {
        LOG_WARN("failed to update task part list", KR(ret), K(tenant_id), K(balance_task),
            K(new_part_list));
      }
    }
  }//end for
  if (OB_SUCC(ret) && !found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not find balance task of transfer partition", KR(ret), K(task), K(balance_tasks));
  }
  return ret;
}

int ObTransferPartitionCommand::execute_cancel_balance_job_(const ObTransferPartitionArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.get_target_tenant_id();
  if (OB_UNLIKELY(!arg.is_valid() || !arg.is_cancel_balance_job())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(check_tenant_status_(tenant_id))) {
    LOG_WARN("failed to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_data_version_for_cancel_(tenant_id))) {
    LOG_WARN("failed to check data version for cancel", KR(ret), K(tenant_id));
  } else {
    ObBalanceJob balance_job;
    int64_t start_time = 0, finish_time = 0;//no used
    START_TRANSACTION(GCTX.sql_proxy_, tenant_id)
    if (FAILEDx(cancel_balance_job_in_trans_(tenant_id, trans))) {
      LOG_WARN("failed to cancel balance job", KR(ret), K(tenant_id));
    }
    END_TRANSACTION(trans)
  }
  return ret;
}

int ObTransferPartitionCommand::cancel_balance_job_in_trans_(
    const uint64_t tenant_id, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id));
  } else {
    ObBalanceJob balance_job;
    int64_t start_time = 0, finish_time = 0;//no used
    if (FAILEDx(ObBalanceJobTableOperator::get_balance_job(tenant_id,
            true, trans, balance_job, start_time, finish_time))) {
      LOG_WARN("failed to get balance job", KR(ret), K(tenant_id));
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "Balance job not exist");
      }
    } else if (balance_job.get_job_status().is_canceling()) {
      //nothing
    } else if (balance_job.get_job_status().is_doing()) {
      if (OB_FAIL(ObBalanceJobTableOperator::update_job_status(tenant_id,
            balance_job.get_job_id(), balance_job.get_job_status(),
            share::ObBalanceJobStatus(share::ObBalanceJobStatus::BALANCE_JOB_STATUS_CANCELING),
            true, "Manual cancel balance job", trans))) {
        LOG_WARN("failed to update job status", KR(ret), K(tenant_id), K(balance_job));
      }
    } else if (balance_job.get_job_status().is_success()
        || balance_job.get_job_status().is_canceled()) {
      //已经执行结束了，不报错，但是也不修改状态
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("balance job is unexpected", KR(ret), K(balance_job));
    }
  }
  return ret;
}
}
}
