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

#define USING_LOG_PREFIX SHARE

#include "share/ob_rpc_struct.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_multi_cluster_util.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/utility/ob_print_utils.h"
#include "rootserver/ob_global_index_builder.h"
#include "common/ob_store_format.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace share::schema;
using namespace share;
using namespace storage;
namespace obrpc {
OB_SERIALIZE_MEMBER(Bool, v_);
OB_SERIALIZE_MEMBER(Int64, v_);
OB_SERIALIZE_MEMBER(UInt64, v_);

static const char* upgrade_stage_str[OB_UPGRADE_STAGE_MAX] = {"NULL", "NONE", "PREUPGRADE", "DBUPGRADE", "POSTUPRADE"};

const char* get_upgrade_stage_str(ObUpgradeStage stage)
{
  const char* str = NULL;
  if (stage > OB_UPGRADE_STAGE_INVALID && stage < OB_UPGRADE_STAGE_MAX) {
    str = upgrade_stage_str[stage];
  } else {
    str = upgrade_stage_str[0];
  }
  return str;
}

ObUpgradeStage get_upgrade_stage(const ObString& str)
{
  ObUpgradeStage stage = OB_UPGRADE_STAGE_INVALID;
  for (int64_t i = OB_UPGRADE_STAGE_NONE; i < OB_UPGRADE_STAGE_MAX; i++) {
    if (0 == str.case_compare(upgrade_stage_str[i])) {
      stage = static_cast<ObUpgradeStage>(i);
      break;
    }
  }
  return stage;
}

int ObDDLArg::assign(const ObDDLArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(primary_schema_versions_.assign(other.primary_schema_versions_))) {
    LOG_WARN("fail to assign primary_schema_versions", KR(ret));
  } else if (OB_FAIL(based_schema_object_infos_.assign(other.based_schema_object_infos_))) {
    LOG_WARN("fail to assign based_schema_object_infos", KR(ret));
  } else {
    ddl_stmt_str_ = other.ddl_stmt_str_;
    exec_tenant_id_ = other.exec_tenant_id_;
    ddl_id_str_ = other.ddl_id_str_;
    is_replay_schema_ = other.is_replay_schema_;
  }
  return ret;
}

DEF_TO_STRING(ObGetRootserverRoleResult)
{
  int64_t pos = 0;
  J_KV(K_(role), K_(replica), K_(status), K_(partition_info));
  return pos;
}

void ObAlterPlanBaselineArg::restore(const ObPlanBaselineInfo& old)
{
  plan_baseline_info_.key_ = old.key_;
  plan_baseline_info_.schema_version_ = old.schema_version_;
  plan_baseline_info_.plan_baseline_id_ = old.plan_baseline_id_;
  plan_baseline_info_.plan_hash_value_ = old.plan_hash_value_;
  if (!(field_update_bitmap_ & FIXED)) {
    plan_baseline_info_.fixed_ = old.fixed_;
  }
  if (!(field_update_bitmap_ & ENABLED)) {
    plan_baseline_info_.enabled_ = old.enabled_;
  }
  if (!(field_update_bitmap_ & OUTLINE_DATA)) {
    plan_baseline_info_.outline_data_ = old.outline_data_;
  }
  plan_baseline_info_.sql_id_ = old.sql_id_;
}

void ObGetRootserverRoleResult::reset()
{
  role_ = 0;
  zone_.reset();
  type_ = REPLICA_TYPE_MAX;
  replica_.reset();
  status_ = status::MAX;
  partition_info_.reset();
}

OB_SERIALIZE_MEMBER(ObGetRootserverRoleResult, role_, zone_, type_, status_, replica_, partition_info_);

DEF_TO_STRING(ObServerInfo)
{
  int64_t pos = 0;
  J_KV("region", region_, "zone", zone_, "server", server_);
  return pos;
}

OB_SERIALIZE_MEMBER(ObServerInfo, zone_, server_, region_);

DEF_TO_STRING(ObPartitionId)
{
  int64_t pos = 0;
  J_KV(KT_(table_id), K_(partition_id));
  return pos;
}

OB_SERIALIZE_MEMBER(ObPartitionId, table_id_, partition_id_);

DEF_TO_STRING(ObPartitionStat)
{
  int64_t pos = 0;
  J_KV(K_(partition_key), K_(stat));
  return pos;
}

OB_SERIALIZE_MEMBER(ObPartitionStat, partition_key_, stat_);

//////////////////////////////////////////////
// ObClonePartitionArg
// DEF_TO_STRING(ObClonePartitionArg)
//{
//  int64_t pos = 0;
//  J_KV(K_(partition_key),
//       K_(migrate_version),
//       K_(last_sstable_index),
//       K_(last_block_index));
//  return pos;
//}
//
// OB_SERIALIZE_MEMBER(ObClonePartitionArg,
//                                 partition_key_,
//                                 migrate_version_,
//                                 last_sstable_index_,
//                                 last_block_index_);
//
//////////////////////////////////////////////

//////////////////////////////////////////////
//
//  Resource Unit & Pool
//
//////////////////////////////////////////////
//
// unit
//
bool ObCreateResourceUnitArg::is_valid() const
{
  // min_xxx be zero means min_xxx doesn't have limit, same with max_xxx
  return !unit_name_.empty() && min_cpu_ >= 0 && min_iops_ >= 0 && min_memory_ >= 0 && max_cpu_ > 0 &&
         max_memory_ > 0 && max_iops_ > 0 && max_disk_size_ > 0 && max_session_num_ > 0;
}

int ObCreateResourceUnitArg::assign(const ObCreateResourceUnitArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    unit_name_ = other.unit_name_;
    min_cpu_ = other.min_cpu_;
    min_iops_ = other.min_iops_;
    min_memory_ = other.min_memory_;
    max_cpu_ = other.max_cpu_;
    max_memory_ = other.max_memory_;
    max_disk_size_ = other.max_disk_size_;
    max_iops_ = other.max_iops_;
    max_session_num_ = other.max_session_num_;
    if_not_exist_ = other.if_not_exist_;
  }
  return ret;
}

DEF_TO_STRING(ObCreateResourceUnitArg)
{
  int64_t pos = 0;
  J_KV(K_(unit_name),
      K_(min_cpu),
      K_(min_iops),
      K_(min_memory),
      K_(max_cpu),
      K_(max_memory),
      K_(max_iops),
      K_(max_disk_size),
      K_(max_session_num),
      K_(if_not_exist));
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateResourceUnitArg, ObDDLArg), unit_name_, min_cpu_, min_iops_, min_memory_, max_cpu_,
    max_memory_, max_iops_, max_disk_size_, max_session_num_, if_not_exist_);

OB_SERIALIZE_MEMBER((ObSplitResourcePoolArg, ObDDLArg), pool_name_, zone_list_, split_pool_list_);

bool ObSplitResourcePoolArg::is_valid() const
{
  return !pool_name_.empty() && zone_list_.count() > 0 && split_pool_list_.count() > 0;
}

int ObSplitResourcePoolArg::assign(const ObSplitResourcePoolArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(zone_list_.assign(other.zone_list_))) {
    LOG_WARN("fail to assign zone_list", KR(ret));
  } else if (OB_FAIL(split_pool_list_.assign(other.split_pool_list_))) {
    LOG_WARN("fail to assign split_pool_list", KR(ret));
  } else {
    pool_name_ = other.pool_name_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObMergeResourcePoolArg, ObDDLArg), old_pool_list_, new_pool_list_);

bool ObMergeResourcePoolArg::is_valid() const
{
  return old_pool_list_.count() > 0 && new_pool_list_.count() > 0 && new_pool_list_.count() < 2;
}

int ObMergeResourcePoolArg::assign(const ObMergeResourcePoolArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(old_pool_list_.assign(other.old_pool_list_))) {
    LOG_WARN("fail to assign old_pool_list", KR(ret));
  } else if (OB_FAIL(new_pool_list_.assign(other.new_pool_list_))) {
    LOG_WARN("fail to assign new_pool_list", KR(ret));
  }
  return ret;
}

bool ObAlterResourceUnitArg::is_valid() const
{
  // 0 means not change
  return !unit_name_.empty() && min_cpu_ >= 0 && min_iops_ >= 0 && min_memory_ >= 0 && max_cpu_ >= 0 &&
         max_memory_ >= 0 && max_iops_ >= 0 && max_disk_size_ >= 0 && max_session_num_ >= 0;
}

int ObAlterResourceUnitArg::assign(const ObAlterResourceUnitArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    unit_name_ = other.unit_name_;
    min_cpu_ = other.min_cpu_;
    min_iops_ = other.min_iops_;
    min_memory_ = other.min_memory_;
    max_cpu_ = other.max_cpu_;
    max_memory_ = other.max_memory_;
    max_disk_size_ = other.max_disk_size_;
    max_iops_ = other.max_iops_;
    max_session_num_ = other.max_session_num_;
  }
  return ret;
}

DEF_TO_STRING(ObAlterResourceUnitArg)
{
  int64_t pos = 0;
  J_KV(K_(unit_name),
      K_(min_cpu),
      K_(min_iops),
      K_(min_memory),
      K_(max_cpu),
      K_(max_memory),
      K_(max_iops),
      K_(max_disk_size),
      K_(max_session_num));
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterResourceUnitArg, ObDDLArg), unit_name_, min_cpu_, min_iops_, min_memory_, max_cpu_,
    max_memory_, max_iops_, max_disk_size_, max_session_num_);

bool ObDropResourceUnitArg::is_valid() const
{
  return !unit_name_.empty();
}

int ObDropResourceUnitArg::assign(const ObDropResourceUnitArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    unit_name_ = other.unit_name_;
    if_exist_ = other.if_exist_;
  }
  return ret;
}

DEF_TO_STRING(ObDropResourceUnitArg)
{
  int64_t pos = 0;
  J_KV(K_(unit_name), K_(if_exist));
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropResourceUnitArg, ObDDLArg), unit_name_, if_exist_);

/// pool

DEF_TO_STRING(ObCreateResourcePoolArg)
{
  int64_t pos = 0;
  J_KV(
      K_(pool_name), K_(unit), K_(unit_num), K_(zone_list), K_(replica_type), K_(if_not_exist), K_(is_tenant_sys_pool));
  return pos;
}

bool ObCreateResourcePoolArg::is_valid() const
{
  // zone_list empty means all zone, don't need to check it
  return !pool_name_.empty() && !unit_.empty() && unit_num_ > 0;
}

int ObCreateResourcePoolArg::assign(const ObCreateResourcePoolArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(zone_list_.assign(other.zone_list_))) {
    LOG_WARN("fail to assign zone_list", KR(ret));
  } else {
    pool_name_ = other.pool_name_;
    unit_ = other.unit_;
    unit_num_ = other.unit_num_;
    if_not_exist_ = other.if_not_exist_;
    replica_type_ = other.replica_type_;
    is_tenant_sys_pool_ = other.is_tenant_sys_pool_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateResourcePoolArg, ObDDLArg), pool_name_, unit_, unit_num_, zone_list_, if_not_exist_,
    replica_type_, is_tenant_sys_pool_);

bool ObAlterResourcePoolArg::is_valid() const
{
  // unit empty means not changed, unit_num zero means not changed,
  // zone_list empty means not changed
  return !pool_name_.empty() && unit_num_ >= 0;
}

int ObAlterResourcePoolArg::assign(const ObAlterResourcePoolArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(zone_list_.assign(other.zone_list_))) {
    LOG_WARN("fail to assign zone_list", KR(ret));
  } else if (OB_FAIL(delete_unit_id_array_.assign(other.delete_unit_id_array_))) {
    LOG_WARN("fail to assign delete_unit_id_array", KR(ret));
  } else {
    pool_name_ = other.pool_name_;
    unit_ = other.unit_;
    unit_num_ = other.unit_num_;
  }
  return ret;
}

DEF_TO_STRING(ObAlterResourcePoolArg)
{
  int64_t pos = 0;
  J_KV(K_(pool_name), K_(unit), K_(unit_num), K_(zone_list), K_(delete_unit_id_array));
  return pos;
}

OB_SERIALIZE_MEMBER(
    (ObAlterResourcePoolArg, ObDDLArg), pool_name_, unit_, unit_num_, zone_list_, delete_unit_id_array_);

bool ObDropResourcePoolArg::is_valid() const
{
  return !pool_name_.empty();
}

int ObDropResourcePoolArg::assign(const ObDropResourcePoolArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    pool_name_ = other.pool_name_;
    if_exist_ = other.if_exist_;
  }
  return ret;
}

DEF_TO_STRING(ObDropResourcePoolArg)
{
  int64_t pos = 0;
  J_KV(K_(pool_name), K_(if_exist));
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropResourcePoolArg, ObDDLArg), pool_name_, if_exist_);

OB_SERIALIZE_MEMBER(ObCmdArg, sql_text_);

OB_SERIALIZE_MEMBER(ObDDLArg, ddl_stmt_str_, exec_tenant_id_, ddl_id_str_, primary_schema_versions_, is_replay_schema_,
    based_schema_object_infos_);

//////////////////////////////////////////////
//
//  Tenant
//
//////////////////////////////////////////////

bool ObSysVarIdValue::is_valid() const
{
  return (SYS_VAR_INVALID != sys_id_);
}

DEF_TO_STRING(ObSysVarIdValue)
{
  int64_t pos = 0;
  J_KV(K_(sys_id), K_(value));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSysVarIdValue, sys_id_, value_);

bool ObCreateTenantArg::is_valid() const
{
  return !tenant_schema_.get_tenant_name_str().empty() && pool_list_.count() > 0;
}

int ObCreateTenantArg::check_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_schema_.get_tenant_name_str().empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name. empty tenant name.");
  } else if (OB_UNLIKELY(pool_list_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "pool list. empty pool list.");
  }
  return ret;
}

int ObCreateTenantArg::assign(const ObCreateTenantArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_schema_.assign(other.tenant_schema_))) {
    LOG_WARN("fail to assign tenant schema", K(ret), K(other));
  } else if (OB_FAIL(pool_list_.assign(other.pool_list_))) {
    LOG_WARN("fail to assign pool list", K(ret), K(other));
  } else if (OB_FAIL(sys_var_list_.assign(other.sys_var_list_))) {
    LOG_WARN("fail to assign sys var list", K(ret), K(other));
  } else if (OB_FAIL(restore_pkeys_.assign(other.restore_pkeys_))) {
    LOG_WARN("fail to assign restore pkeys", K(ret), K(other));
  } else if (OB_FAIL(restore_log_pkeys_.assign(other.restore_log_pkeys_))) {
    LOG_WARN("fail to assign log restore pkeys", K(ret), K(other));
  } else {
    if_not_exist_ = other.if_not_exist_;
    name_case_mode_ = other.name_case_mode_;
    is_restore_ = other.is_restore_;
    restore_frozen_status_ = other.restore_frozen_status_;
  }
  return ret;
}

DEF_TO_STRING(ObCreateTenantArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_schema),
      K_(pool_list),
      K_(if_not_exist),
      K_(sys_var_list),
      K_(name_case_mode),
      K_(is_restore),
      K_(restore_pkeys),
      K_(restore_log_pkeys),
      K_(restore_frozen_status));
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateTenantArg, ObDDLArg), tenant_schema_, pool_list_, if_not_exist_, sys_var_list_,
    name_case_mode_, is_restore_, restore_pkeys_, restore_log_pkeys_, restore_frozen_status_);

bool ObCreateTenantEndArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}

DEF_TO_STRING(ObCreateTenantEndArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id));
  return pos;
}

int ObCreateTenantEndArg::assign(const ObCreateTenantEndArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    tenant_id_ = other.tenant_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateTenantEndArg, ObDDLArg), tenant_id_);

bool ObModifyTenantArg::is_valid() const
{
  // empty pool_list means not changed
  return !tenant_schema_.get_tenant_name_str().empty();
}

int ObModifyTenantArg::check_normal_tenant_can_do(bool& normal_can_do) const
{
  int ret = OB_SUCCESS;
  normal_can_do = false;
  common::ObBitSet<> normal_ops;
  if (OB_FAIL(normal_ops.add_member(READ_ONLY))) {
    LOG_WARN("Failed to add member READ_ONLY", K(ret));
  } else if (OB_FAIL(normal_ops.add_member(PRIMARY_ZONE))) {
    LOG_WARN("Failed to add memeber PRIMARY ZONE", K(ret));
  } else {
    normal_can_do = alter_option_bitset_.is_subset(normal_ops);
  }
  return ret;
}

bool ObModifyTenantArg::is_allow_when_disable_ddl() const
{
  bool bret = false;
  if (alter_option_bitset_.is_empty()) {
    bret = false;
  } else {
    bret = true;
    for (int64_t i = 0; i < MAX_OPTION && bret; i++) {
      if (alter_option_bitset_.has_member(i) && i != PRIMARY_ZONE && i != ZONE_LIST && i != RESOURCE_POOL_LIST) {
        bret = false;
      }
    }
  }
  return bret;
}

bool ObModifyTenantArg::is_allow_when_upgrade() const
{
  return !alter_option_bitset_.is_empty();
}

DEF_TO_STRING(ObModifyTenantArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_schema), K_(pool_list), K_(alter_option_bitset), K_(sys_var_list), K_(new_tenant_name));
  return pos;
}

OB_SERIALIZE_MEMBER(
    (ObModifyTenantArg, ObDDLArg), tenant_schema_, alter_option_bitset_, pool_list_, sys_var_list_, new_tenant_name_);

bool ObLockTenantArg::is_valid() const
{
  return !tenant_name_.empty();
}

DEF_TO_STRING(ObLockTenantArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_name), K_(is_locked));
  return pos;
}

OB_SERIALIZE_MEMBER((ObLockTenantArg, ObDDLArg), tenant_name_, is_locked_);

bool ObDropTenantArg::is_valid() const
{
  return !tenant_name_.empty();
}

DEF_TO_STRING(ObDropTenantArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_name), K_(if_exist), K_(delay_to_drop), K_(force_drop), K_(object_name), K_(open_recyclebin));
  return pos;
}

OB_SERIALIZE_MEMBER(
    (ObDropTenantArg, ObDDLArg), tenant_name_, if_exist_, delay_to_drop_, force_drop_, object_name_, open_recyclebin_);

bool ObAddSysVarArg::is_valid() const
{
  return sysvar_.is_valid();
}

OB_SERIALIZE_MEMBER((ObAddSysVarArg, ObDDLArg), sysvar_, if_not_exist_);

DEF_TO_STRING(ObAddSysVarArg)
{
  int64_t pos = 0;
  J_KV(K_(sysvar), K_(if_not_exist));
  return pos;
}

bool ObModifySysVarArg::is_valid() const
{
  return !sys_var_list_.empty() && OB_INVALID_ID != tenant_id_;
}

int ObModifySysVarArg::assign(const ObModifySysVarArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret), K(other));
  } else if (OB_FAIL(sys_var_list_.assign(other.sys_var_list_))) {
    LOG_WARN("fail to assign sys_var_list", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    is_inner_ = other.is_inner_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObModifySysVarArg, ObDDLArg), tenant_id_, sys_var_list_, is_inner_);

DEF_TO_STRING(ObModifySysVarArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(sys_var_list), K_(is_inner));
  return pos;
}

bool ObCreateDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != database_schema_.get_tenant_id() && !database_schema_.get_database_name_str().empty();
}

DEF_TO_STRING(ObCreateDatabaseArg)
{
  int64_t pos = 0;
  J_KV(K_(database_schema), K_(if_not_exist));
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateDatabaseArg, ObDDLArg), database_schema_, if_not_exist_);

bool ObAlterDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != database_schema_.get_tenant_id() && !database_schema_.get_database_name_str().empty();
}

DEF_TO_STRING(ObAlterDatabaseArg)
{
  int64_t pos = 0;
  J_KV(K_(database_schema));
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterDatabaseArg, ObDDLArg), database_schema_, alter_option_bitset_);

bool ObDropDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !database_name_.empty();
}

DEF_TO_STRING(ObDropDatabaseArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(database_name), K_(if_exist), K_(to_recyclebin));
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropDatabaseArg, ObDDLArg), tenant_id_, database_name_, if_exist_, to_recyclebin_);

bool ObCreateTablegroupArg::is_valid() const
{
  return OB_INVALID_ID != tablegroup_schema_.get_tenant_id() && !tablegroup_schema_.get_tablegroup_name().empty();
}

DEF_TO_STRING(ObCreateTablegroupArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tablegroup_schema), K_(if_not_exist), K_(create_mode));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateTablegroupArg, ObDDLArg), tablegroup_schema_, if_not_exist_, create_mode_)

bool ObDropTablegroupArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !tablegroup_name_.empty();
}

DEF_TO_STRING(ObDropTablegroupArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(tablegroup_name), K_(if_exist));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropTablegroupArg, ObDDLArg), tenant_id_, tablegroup_name_, if_exist_);

bool ObAlterTablegroupArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !tablegroup_name_.empty();
}

bool ObAlterTablegroupArg::is_alter_partitions() const
{
  return alter_option_bitset_.has_member(ADD_PARTITION) || alter_option_bitset_.has_member(DROP_PARTITION) ||
         alter_option_bitset_.has_member(PARTITIONED_TABLE) || alter_option_bitset_.has_member(REORGANIZE_PARTITION) ||
         alter_option_bitset_.has_member(SPLIT_PARTITION);
}

bool ObAlterTablegroupArg::is_allow_when_disable_ddl() const
{
  bool bret = false;
  if (alter_option_bitset_.is_empty()) {
    bret = false;
  } else {
    bret = true;
    for (int64_t i = 0; i < MAX_OPTION && bret; i++) {
      if (alter_option_bitset_.has_member(i) && i != PRIMARY_ZONE) {
        bret = false;
      }
    }
  }
  return bret;
}

bool ObAlterTablegroupArg::is_allow_when_upgrade() const
{
  bool bret = false;
  if (alter_option_bitset_.is_empty()) {
    bret = false;
  } else {
    bret = true;
    for (int64_t i = 0; i < MAX_OPTION && bret; i++) {
      if (alter_option_bitset_.has_member(i) && i != PRIMARY_ZONE && i != LOCALITY && i != FORCE_LOCALITY) {
        bret = false;
      }
    }
  }
  return bret;
}

DEF_TO_STRING(ObAlterTablegroupArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_items), K_(tenant_id), K_(tablegroup_name), K_(create_mode), K_(alter_tablegroup_schema));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterTablegroupArg, ObDDLArg), tenant_id_, tablegroup_name_, table_items_, alter_option_bitset_,
    create_mode_, alter_tablegroup_schema_);

bool ObCreateTableArg::is_valid() const
{
  // index_arg_list can be empty
  return OB_INVALID_ID != schema_.get_tenant_id() && !schema_.get_table_name_str().empty();
}

DEF_TO_STRING(ObCreateTableArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(if_not_exist),
      K_(schema),
      K_(index_arg_list),
      K_(constraint_list),
      K_(db_name),
      K_(last_replay_log_id),
      K_(foreign_key_arg_list),
      K_(is_inner),
      K_(error_info));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateTableArg, ObDDLArg), if_not_exist_, schema_, index_arg_list_, db_name_, create_mode_,
    foreign_key_arg_list_, constraint_list_, last_replay_log_id_, is_inner_, error_info_);

bool ObCreateTableArg::is_allow_when_upgrade() const
{
  bool bret = true;
  if (0 != foreign_key_arg_list_.count()) {
    bret = false;
  } else {
    for (int64_t i = 0; bret && i < constraint_list_.count(); i++) {
      if (CONSTRAINT_TYPE_PRIMARY_KEY != constraint_list_.at(i).get_constraint_type()) {
        bret = false;
      }
    }
  }
  return bret;
}

OB_SERIALIZE_MEMBER(ObCreateTableRes, table_id_, schema_version_);

bool ObCreateTableLikeArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !origin_db_name_.empty() && !origin_table_name_.empty() &&
         !new_db_name_.empty() && !new_table_name_.empty() &&
         (table_type_ == USER_TABLE || table_type_ == TMP_TABLE_ORA_SESS || table_type_ == TMP_TABLE);
}

DEF_TO_STRING(ObCreateTableLikeArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(if_not_exist),
      K_(tenant_id),
      K_(origin_db_name),
      K_(origin_table_name),
      K_(new_db_name),
      K_(new_table_name),
      K_(table_type),
      K_(create_host));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateTableLikeArg, ObDDLArg), if_not_exist_, tenant_id_, origin_db_name_, origin_table_name_,
    new_db_name_, new_table_name_, create_mode_, table_type_, create_host_);

bool ObAlterTableArg::is_valid() const
{
  // TODO: add more check if needed
  if (is_refresh_sess_active_time()) {
    return true;
  } else {
    return OB_INVALID_ID != alter_table_schema_.get_tenant_id() && !alter_table_schema_.origin_database_name_.empty() &&
           !alter_table_schema_.origin_table_name_.empty();
  }
}

bool ObAlterTableArg::is_refresh_sess_active_time() const
{
  return (alter_table_schema_.alter_option_bitset_.has_member(SESSION_ACTIVE_TIME) &&
          OB_DDL_ALTER_TABLE == alter_table_schema_.alter_type_ && OB_INVALID_ID != session_id_);
}

bool ObAlterTableArg::is_allow_when_disable_ddl() const
{
  bool bret = false;
  if (alter_table_schema_.alter_option_bitset_.is_empty()) {
    bret = false;
  } else {
    bret = true;
    for (int64_t i = 0; i < MAX_OPTION && bret && is_alter_options_; i++) {
      if (alter_table_schema_.alter_option_bitset_.has_member(i) && i != PRIMARY_ZONE) {
        bret = false;
      }
    }
  }
  return bret;
}

bool ObAlterTableArg::is_allow_when_upgrade() const
{
  bool bret = false;
  if (alter_table_schema_.alter_option_bitset_.is_empty() && !is_alter_columns_ && !is_alter_indexs_) {
    bret = false;
  } else {
    bret = true;
    if (is_alter_indexs_) {
      for (int64_t i = 0; bret && i < index_arg_list_.count(); i++) {
        if (OB_ISNULL(index_arg_list_.at(i))) {
          bret = false;
          LOG_WARN("ptr is null", K(bret));
        } else {
          bret = index_arg_list_.at(i)->is_allow_when_upgrade();
        }
      }
    }
    for (int64_t i = 0; i < MAX_OPTION && bret && is_alter_options_; i++) {
      if (alter_table_schema_.alter_option_bitset_.has_member(i) && i != PRIMARY_ZONE && i != LOCALITY &&
          i != FORCE_LOCALITY) {
        bret = false;
      }
    }
    if (is_alter_columns_ && bret) {
      // Only add columns and extend the length of the columns will be allowed again in ddl_service
      ObTableSchema::const_column_iterator it_begin = alter_table_schema_.column_begin();
      ObTableSchema::const_column_iterator it_end = alter_table_schema_.column_end();
      AlterColumnSchema* alter_column_schema = NULL;
      for (; bret && it_begin != it_end; it_begin++) {
        if (OB_ISNULL(*it_begin)) {
          bret = false;
          LOG_WARN("*it_begin is NULL", K(bret));
        } else {
          alter_column_schema = static_cast<AlterColumnSchema*>(*it_begin);
          // mysql mode, OB_ALL_MODIFY_COLUMN function is a subset of OB_ALL_CHANGE_COLUMN;
          // Oracle mode, only OB_ALL_MODIFY_COLUMN. In the case of only supporting extended column length, for
          // simplicity of implementation, only OB_ALL_MODIFY_COLUMN is left here.
          if (OB_DDL_MODIFY_COLUMN != alter_column_schema->alter_type_ &&
              OB_DDL_ADD_COLUMN != alter_column_schema->alter_type_) {
            bret = false;
          }
        }
      }
    }
  }
  return bret;
}

int ObAlterTableArg::set_nls_formats(const common::ObString* nls_formats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(nls_formats)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char* tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; ++i) {
      if (OB_ISNULL(tmp_ptr[i] = (char*)allocator_.alloc(nls_formats[i].length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to alloc memory!", "size", nls_formats[i].length(), K(ret));
      } else {
        MEMCPY(tmp_ptr[i], nls_formats[i].ptr(), nls_formats[i].length());
        nls_formats_[i].assign_ptr(tmp_ptr[i], nls_formats[i].length());
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
        allocator_.free(tmp_ptr[i]);
      }
    }
  }
  return ret;
}

int ObAlterTableArg::serialize_index_args(char* buf, const int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (!is_valid() || NULL == buf || data_len <= 0 || pos >= data_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this, KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, index_arg_list_.size()))) {
    SHARE_LOG(WARN, "Fail to serialize index arg count", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < index_arg_list_.size(); ++i) {
    ObIndexArg* index_arg = index_arg_list_.at(i);
    if (index_arg->index_action_type_ == ObIndexArg::ADD_INDEX) {
      ObCreateIndexArg* create_index_arg = static_cast<ObCreateIndexArg*>(index_arg);
      if (NULL == create_index_arg) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos, create_index_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(create_index_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize create index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::DROP_INDEX) {
      ObDropIndexArg* drop_index_arg = static_cast<ObDropIndexArg*>(index_arg);
      if (NULL == drop_index_arg) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos, drop_index_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(drop_index_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize drop index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::ALTER_INDEX) {
      ObAlterIndexArg* alter_index_arg = static_cast<ObAlterIndexArg*>(index_arg);
      if (OB_UNLIKELY(NULL == alter_index_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos, alter_index_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(alter_index_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize alter index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::ALTER_INDEX_PARALLEL) {
      ObAlterIndexParallelArg* alter_index_parallel_arg = static_cast<ObAlterIndexParallelArg*>(index_arg);
      if (OB_UNLIKELY(NULL == alter_index_parallel_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(
                     serialization::encode_vi32(buf, data_len, pos, alter_index_parallel_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(alter_index_parallel_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize alter index parallel arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::RENAME_INDEX) {
      ObRenameIndexArg* rename_index_arg = static_cast<ObRenameIndexArg*>(index_arg);
      SHARE_LOG(WARN,
          "serialize rename index arg!",
          K(rename_index_arg->origin_index_name_),
          K(rename_index_arg->new_index_name_));

      if (OB_UNLIKELY(NULL == rename_index_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos, rename_index_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(rename_index_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize alter index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::DROP_FOREIGN_KEY) {
      ObDropForeignKeyArg* foreign_key_arg = static_cast<ObDropForeignKeyArg*>(index_arg);
      if (OB_UNLIKELY(NULL == foreign_key_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos, foreign_key_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(foreign_key_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize drop foreign key arg!", K(data_len), K(pos), K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "unknown index action type", K_(index_arg->index_action_type), K(ret));
    }
  }
  return ret;
}

int ObAlterTableArg::deserialize_index_args(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    // do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    SHARE_LOG(WARN, "Fail to decode column count", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObIndexArg::IndexActionType index_action_type = ObIndexArg::INVALID_ACTION;
    if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&index_action_type))))) {
      SHARE_LOG(WARN, "failed to decode index action type", K(ret));
      break;
    }
    void* tmp_ptr = NULL;
    if (index_action_type == ObIndexArg::ADD_INDEX) {
      ObCreateIndexArg* create_index_arg = NULL;
      if (NULL == (tmp_ptr = (ObCreateIndexArg*)allocator_.alloc(sizeof(ObCreateIndexArg)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to alloc memory!", K(ret));
      } else {
        create_index_arg = new (tmp_ptr) ObCreateIndexArg();
        if (OB_FAIL(create_index_arg->deserialize(buf, data_len, pos))) {
          SHARE_LOG(WARN, "failed to deserialize create index arg!", K(ret));
        } else if (OB_FAIL(index_arg_list_.push_back(create_index_arg))) {
          SHARE_LOG(WARN, "failed to add to index arg list", K(ret));
        }
        if (OB_FAIL(ret)) {
          create_index_arg->~ObCreateIndexArg();
          allocator_.free(static_cast<char*>(tmp_ptr));
          create_index_arg = NULL;
          tmp_ptr = NULL;
        }
      }
    } else if (index_action_type == ObIndexArg::DROP_INDEX) {
      ObDropIndexArg* drop_index_arg = NULL;
      if (NULL == (tmp_ptr = (ObDropIndexArg*)allocator_.alloc(sizeof(ObDropIndexArg)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
      } else {
        drop_index_arg = new (tmp_ptr) ObDropIndexArg();
        if (OB_FAIL(drop_index_arg->deserialize(buf, data_len, pos))) {
          SHARE_LOG(WARN, "failed to deserialize drop index arg!", K(ret));
        } else if (OB_FAIL(index_arg_list_.push_back(drop_index_arg))) {
          SHARE_LOG(WARN, "failed to add to index arg list", K(ret));
        }
        if (OB_FAIL(ret)) {
          drop_index_arg->~ObDropIndexArg();
          allocator_.free(static_cast<char*>(tmp_ptr));
          drop_index_arg = NULL;
          tmp_ptr = NULL;
        }
      }
    } else if (index_action_type == ObIndexArg::ALTER_INDEX) {
      ObAlterIndexArg* alter_index_arg = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexArg*)allocator_.alloc(sizeof(ObAlterIndexArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
      } else {
        alter_index_arg = new (tmp_ptr) ObAlterIndexArg();
        if (OB_FAIL(alter_index_arg->deserialize(buf, data_len, pos))) {
          SHARE_LOG(WARN, "failed to deserialize alter index arg!", K(ret));
        } else if (OB_FAIL(index_arg_list_.push_back(alter_index_arg))) {
          SHARE_LOG(WARN, "failed to add to index arg list", K(ret));
        }
        if (OB_FAIL(ret)) {
          alter_index_arg->~ObAlterIndexArg();
          allocator_.free(static_cast<char*>(tmp_ptr));
          alter_index_arg = NULL;
          tmp_ptr = NULL;
        }
      }
    } else if (index_action_type == ObIndexArg::ALTER_INDEX_PARALLEL) {
      ObAlterIndexParallelArg* alter_index_parallel_arg = NULL;
      if (OB_UNLIKELY(
              NULL == (tmp_ptr = (ObAlterIndexParallelArg*)allocator_.alloc(sizeof(ObAlterIndexParallelArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
      } else {
        alter_index_parallel_arg = new (tmp_ptr) ObAlterIndexParallelArg();
        if (OB_FAIL(alter_index_parallel_arg->deserialize(buf, data_len, pos))) {
          SHARE_LOG(WARN, "failed to deserialize rename index arg!", K(ret));
        } else if (OB_FAIL(index_arg_list_.push_back(alter_index_parallel_arg))) {
          SHARE_LOG(WARN, "failed to alter index parallel to index arg list", K(ret));
        }
        if (OB_FAIL(ret)) {
          alter_index_parallel_arg->~ObAlterIndexParallelArg();
          allocator_.free(static_cast<char*>(tmp_ptr));
          alter_index_parallel_arg = NULL;
          tmp_ptr = NULL;
        }
      }
    } else if (index_action_type == ObIndexArg::RENAME_INDEX) {
      ObRenameIndexArg* rename_index_arg = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObRenameIndexArg*)allocator_.alloc(sizeof(ObRenameIndexArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
      } else {
        rename_index_arg = new (tmp_ptr) ObRenameIndexArg();
        if (OB_FAIL(rename_index_arg->deserialize(buf, data_len, pos))) {
          SHARE_LOG(WARN, "failed to deserialize rename index arg!", K(ret));
        } else if (OB_FAIL(index_arg_list_.push_back(rename_index_arg))) {
          SHARE_LOG(WARN, "failed to rename to index arg list", K(ret));
        }
        if (OB_FAIL(ret)) {
          rename_index_arg->~ObRenameIndexArg();
          allocator_.free(static_cast<char*>(tmp_ptr));
          rename_index_arg = NULL;
          tmp_ptr = NULL;
        }
      }
    } else if (index_action_type == ObIndexArg::DROP_FOREIGN_KEY) {
      ObDropForeignKeyArg* drop_foreign_key_arg = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObDropForeignKeyArg*)allocator_.alloc(sizeof(ObDropForeignKeyArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
      } else {
        drop_foreign_key_arg = new (tmp_ptr) ObDropForeignKeyArg();
        if (OB_FAIL(drop_foreign_key_arg->deserialize(buf, data_len, pos))) {
          SHARE_LOG(WARN, "failed to deserialize drop foreign key arg!", K(ret));
        } else if (OB_FAIL(index_arg_list_.push_back(drop_foreign_key_arg))) {
          SHARE_LOG(WARN, "failed to add to index arg list", K(ret));
        }
        if (OB_FAIL(ret)) {
          drop_foreign_key_arg->~ObDropForeignKeyArg();
          allocator_.free(static_cast<char*>(tmp_ptr));
          drop_foreign_key_arg = NULL;
          tmp_ptr = NULL;
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "unknown index action type", K(index_action_type), K(ret));
    }
  }
  return ret;
}

int64_t ObAlterTableArg::get_index_args_serialize_size() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else {
    len += serialization::encoded_length_vi64(index_arg_list_.size());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list_.size(); ++i) {
    ObIndexArg* index_arg = index_arg_list_.at(i);
    if (NULL == index_arg) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "index arg should not be null", K(ret));
    } else {
      len += serialization::encoded_length(index_arg->index_action_type_);
      if (ObIndexArg::ADD_INDEX == index_arg->index_action_type_) {
        ObCreateIndexArg* create_index_arg = static_cast<ObCreateIndexArg*>(index_arg);
        if (NULL == create_index_arg) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += create_index_arg->get_serialize_size();
        }
      } else if (ObIndexArg::DROP_INDEX == index_arg->index_action_type_) {
        ObDropIndexArg* drop_index_arg = static_cast<ObDropIndexArg*>(index_arg);
        if (NULL == drop_index_arg) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += drop_index_arg->get_serialize_size();
        }
      } else if (ObIndexArg::ALTER_INDEX == index_arg->index_action_type_) {
        ObAlterIndexArg* alter_index_arg = static_cast<ObAlterIndexArg*>(index_arg);
        if (OB_UNLIKELY(NULL == alter_index_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += alter_index_arg->get_serialize_size();
        }
      } else if (ObIndexArg::DROP_FOREIGN_KEY == index_arg->index_action_type_) {
        ObDropForeignKeyArg* drop_foreign_key_arg = static_cast<ObDropForeignKeyArg*>(index_arg);
        if (OB_UNLIKELY(NULL == drop_foreign_key_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "foreign key arg is null", K(ret));
        } else {
          len += drop_foreign_key_arg->get_serialize_size();
        }
      } else if (ObIndexArg::ALTER_INDEX_PARALLEL == index_arg->index_action_type_) {
        ObAlterIndexParallelArg* alter_index_parallel_arg = static_cast<ObAlterIndexParallelArg*>(index_arg);
        if (OB_UNLIKELY(NULL == alter_index_parallel_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += alter_index_parallel_arg->get_serialize_size();
        }
      } else if (ObIndexArg::RENAME_INDEX == index_arg->index_action_type_) {
        ObRenameIndexArg* rename_index_arg = static_cast<ObRenameIndexArg*>(index_arg);
        if (OB_UNLIKELY(NULL == rename_index_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += rename_index_arg->get_serialize_size();
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "Invalid index action type!", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

OB_DEF_SERIALIZE(ObAlterTableArg)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else if (OB_FAIL(ObDDLArg::serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize DDLArg", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, is_alter_columns_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize is_alter_columns", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, is_alter_indexs_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize is_alter_indexes", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, is_alter_options_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize is_alter_options", K(ret));
  } else if (OB_FAIL(serialize_index_args(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize index args", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(alter_table_schema_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize alter table schema", K(ret));
  } else if (OB_FAIL(tz_info_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize timezone info", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, is_alter_partitions_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize is_alter_partitions", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, alter_part_type_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize alter_part_type", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, create_mode_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize create_mode", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, alter_constraint_type_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize alter_constraint_type", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, session_id_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize session_id", K(ret));
  } else if (OB_FAIL(tz_info_wrap_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize timezone info wrap", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, is_inner_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to encode skip is_inner", K(ret));
  } else {
    for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
      if (OB_FAIL(nls_formats_[i].serialize(buf, buf_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to serialize nls_formats_[i]", K(nls_formats_[i]), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, skip_sys_table_check_))) {
      SHARE_SCHEMA_LOG(WARN, "fail to encode skip sys table check", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(foreign_key_arg_list_.serialize(buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to serialize foreign_key_arg_list_", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, is_update_global_indexes_))) {
      SHARE_SCHEMA_LOG(WARN, "fail to serialize is_update_global_indexes", KR(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObAlterTableArg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize DDLArg", K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&is_alter_columns_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_type_, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&is_alter_indexs_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_type_, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&is_alter_options_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_type_, ", K(ret));
  } else if (OB_FAIL(deserialize_index_args(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize index args, ", K(ret));
  } else if (OB_FAIL(alter_table_schema_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter table schema, ", K(ret));
  } else if (OB_FAIL(tz_info_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize timezone info", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&is_alter_partitions_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_partitions_, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&alter_part_type_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_part_type_, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&create_mode_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize create_mode_, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&alter_constraint_type_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_constraint_type_, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, ((int64_t*)(&session_id_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize session_id_, ", K(ret));
  } else if (pos < data_len) {
    if (OB_FAIL(tz_info_wrap_.deserialize(buf, data_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to deserialize timezone info", K(ret));
    }
  } else {
    tz_info_wrap_.set_tz_info_offset(tz_info_.get_offset());
    tz_info_wrap_.set_error_on_overlap_time(tz_info_.is_error_on_overlap_time());
  }

  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&is_inner_))))) {
      SHARE_SCHEMA_LOG(WARN, "fail to decode skip is_inner", K(ret));
    }
  }

  if (OB_SUCC(ret) && pos < data_len) {
    ObString tmp_string;
    char* tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; ++i) {
      if (OB_FAIL(tmp_string.deserialize(buf, data_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to deserialize nls_formats_", K(i), K(ret));
      } else if (OB_ISNULL(tmp_ptr[i] = (char*)allocator_.alloc(tmp_string.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(ERROR, "failed to alloc memory!", "size", tmp_string.length(), K(ret));
      } else {
        MEMCPY(tmp_ptr[i], tmp_string.ptr(), tmp_string.length());
        nls_formats_[i].assign_ptr(tmp_ptr[i], tmp_string.length());
        tmp_string.reset();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
        allocator_.free(tmp_ptr[i]);
      }
    }
  } else {
    nls_formats_[ObNLSFormatEnum::NLS_DATE] = ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT;
    nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
    nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  }

  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&skip_sys_table_check_))))) {
      SHARE_SCHEMA_LOG(WARN, "fail to decode skip sys table check", K(ret));
    }
  }

  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(foreign_key_arg_list_.deserialize(buf, data_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to deserialize foreign_key_arg_list_", K(ret));
    }
  }

  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t*)(&is_update_global_indexes_))))) {
      SHARE_SCHEMA_LOG(WARN, "fail to deserialize is_update_global_indexes, ", KR(ret));
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObAlterTableArg)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else {
    len += ObDDLArg::get_serialize_size();
    len += serialization::encoded_length_vi32(is_alter_columns_);
    len += serialization::encoded_length_vi32(is_alter_indexs_);
    len += serialization::encoded_length_vi32(is_alter_partitions_);
    len += serialization::encoded_length_vi32(is_alter_options_);
    len += get_index_args_serialize_size();
    len += alter_table_schema_.get_serialize_size();
    len += tz_info_.get_serialize_size();
    len += serialization::encoded_length_vi32(alter_part_type_);
    len += serialization::encoded_length_vi32(alter_constraint_type_);
    len += serialization::encoded_length_vi32(create_mode_);
    len += serialization::encoded_length_vi64(session_id_);
    len += tz_info_wrap_.get_serialize_size();
    len += serialization::encoded_length_vi32(is_inner_);
    for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
      len += nls_formats_[i].get_serialize_size();
    }
    len += serialization::encoded_length_vi32(skip_sys_table_check_);
    len += foreign_key_arg_list_.get_serialize_size();
    len += serialization::encoded_length_vi32(is_update_global_indexes_);
  }

  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

bool ObTruncateTableArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !database_name_.empty() && !table_name_.empty();
}

OB_SERIALIZE_MEMBER(
    (ObTruncateTableArg, ObDDLArg), tenant_id_, database_name_, table_name_, create_mode_, to_recyclebin_, session_id_);

DEF_TO_STRING(ObTruncateTableArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(database_name), K_(table_name), K_(to_recyclebin), K_(session_id));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObRenameTableItem, origin_db_name_, new_db_name_, origin_table_name_, new_table_name_);

DEF_TO_STRING(ObRenameTableItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(origin_db_name), K_(new_db_name), K_(origin_table_name), K_(new_table_name), K_(origin_table_id));
  J_OBJ_END();
  return pos;
}

bool ObRenameTableItem::is_valid() const
{
  return !origin_db_name_.empty() && !new_db_name_.empty() && !origin_table_name_.empty() && !new_table_name_.empty();
}

bool ObRenameTableArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && rename_table_items_.count() > 0;
}

DEF_TO_STRING(ObRenameTableArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(rename_table_items));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObRenameTableArg, ObDDLArg), tenant_id_, rename_table_items_);

DEF_TO_STRING(ObTableItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(database_name), K_(table_name));
  J_OBJ_END();
  return pos;
}

bool ObTableItem::operator==(const ObTableItem& r) const
{
  bool ret = false;
  if (OB_NAME_CASE_INVALID != mode_ && mode_ == r.mode_) {
    if (!table_name_.empty() && !r.table_name_.empty() && !database_name_.empty() && !r.database_name_.empty()) {
      // todo compare using case mode @hualong
      // ret = ObCharset::case_mode_equal(mode_, table_name_, r.table_name_) &&
      //    ObCharset::case_mode_equal(mode_, database_name_, r.database_name_);
      ret = table_name_ == r.table_name_ && database_name_ == r.database_name_;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTableItem, database_name_, table_name_);

bool ObDropTableArg::is_valid() const
{
  bool ret = (OB_INVALID_ID != tenant_id_ && table_type_ < MAX_TABLE_TYPE && tables_.count() > 0);
  if (false == ret && (TMP_TABLE == table_type_ || TMP_TABLE_ORA_TRX == table_type_ ||
                          TMP_TABLE_ORA_SESS == table_type_ || TMP_TABLE_ALL == table_type_)) {
    LOG_WARN("drop table valid check for temp table");
    ret = (session_id_ != OB_INVALID_ID && true == if_exist_ && false == to_recyclebin_);
  }
  return ret;
}

DEF_TO_STRING(ObDropTableArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(
      K_(tenant_id), K_(table_type), K_(tables), K_(if_exist), K_(to_recyclebin), K_(session_id), K_(sess_create_time));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropTableArg, ObDDLArg), tenant_id_, table_type_, tables_, if_exist_, to_recyclebin_,
    session_id_, sess_create_time_);

bool ObOptimizeTableArg::is_valid() const
{
  return (OB_INVALID_ID != tenant_id_ && tables_.count() > 0);
}

OB_SERIALIZE_MEMBER((ObOptimizeTableArg, ObDDLArg), tenant_id_, tables_);

DEF_TO_STRING(ObOptimizeTableArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(tables));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObOptimizeTenantArg, ObDDLArg), tenant_name_);

bool ObOptimizeTenantArg::is_valid() const
{
  return !tenant_name_.empty();
}

DEF_TO_STRING(ObOptimizeTenantArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_name));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObOptimizeAllArg, ObDDLArg));

DEF_TO_STRING(ObOptimizeAllArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObColumnSortItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(column_name), K_(prefix_len), K_(order_type), K_(column_id), K_(is_func_index));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObColumnSortItem, column_name_, prefix_len_, order_type_, column_id_, is_func_index_);

bool ObTableOption::is_valid() const
{
  // if replica_num not set, it's default value is zero
  return block_size_ > 0 && replica_num_ >= 0 && index_status_ > INDEX_STATUS_NOT_FOUND &&
         index_status_ < INDEX_STATUS_MAX && !compress_method_.empty() && progressive_merge_num_ >= 0 &&
         ObStoreFormat::is_store_format_valid(store_format_) && ObStoreFormat::is_row_store_type_valid(row_store_type_);
}

bool ObIndexOption::is_valid() const
{
  // if replica_num not set, it's default value is zero
  return block_size_ > 0 && index_status_ > INDEX_STATUS_NOT_FOUND && index_status_ < INDEX_STATUS_MAX &&
         progressive_merge_num_ >= 0;
}

DEF_TO_STRING(ObTableOption)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(block_size),
      K_(replica_num),
      K_(index_status),
      K_(use_bloom_filter),
      K_(compress_method),
      K_(comment),
      K_(tablegroup_name),
      K_(progressive_merge_num),
      K_(primary_zone),
      K_(row_store_type),
      K_(store_format));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObTableOption, block_size_, replica_num_, index_status_, use_bloom_filter_, compress_method_,
    comment_, progressive_merge_num_, row_store_type_, store_format_);

DEF_TO_STRING(ObIndexOption)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(block_size),
      K_(replica_num),
      K_(index_status),
      K_(use_bloom_filter),
      K_(compress_method),
      K_(comment),
      K_(tablegroup_name),
      K_(progressive_merge_num),
      K_(primary_zone),
      K_(parser_name),
      K_(index_attributes_set),
      K_(row_store_type),
      K_(store_format));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObIndexOption, ObTableOption), parser_name_, index_attributes_set_);

bool ObIndexArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !index_name_.empty() && !table_name_.empty() && !database_name_.empty() &&
         INVALID_ACTION != index_action_type_;
}

bool ObIndexArg::is_allow_when_upgrade() const
{
  return ADD_INDEX == index_action_type_ || DROP_INDEX == index_action_type_ || DROP_FOREIGN_KEY == index_action_type_;
}

DEF_TO_STRING(ObIndexArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(session_id), K_(index_name), K_(table_name), K_(database_name), K_(index_action_type));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(
    (ObIndexArg, ObDDLArg), tenant_id_, index_name_, table_name_, database_name_, index_action_type_, session_id_);

bool ObCreateIndexArg::is_valid() const
{
  // store_columns_ can be empty
  return ObIndexArg::is_valid() && index_type_ > INDEX_TYPE_IS_NOT && index_type_ < INDEX_TYPE_MAX &&
         index_columns_.count() > 0 && index_option_.is_valid() && index_using_type_ >= USING_BTREE &&
         index_using_type_ < USING_TYPE_MAX;
}

DEF_TO_STRING(ObCreateIndexArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME(N_INDEX_ARG);
  J_COLON();
  pos += ObIndexArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(index_type),
      K_(index_columns),
      K_(fulltext_columns),
      K_(store_columns),
      K_(index_option),
      K_(index_using_type),
      K_(data_table_id),
      K_(index_table_id),
      K_(if_not_exist),
      K_(index_schema),
      K_(is_inner),
      K_(nls_date_format),
      K_(nls_timestamp_format),
      K_(nls_timestamp_tz_format),
      K_(sql_mode));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateIndexArg, ObIndexArg), index_type_, index_columns_, store_columns_, index_option_,
    index_using_type_, fulltext_columns_, create_mode_, data_table_id_, index_table_id_, if_not_exist_, with_rowid_,
    index_schema_, is_inner_, hidden_store_columns_, nls_date_format_, nls_timestamp_format_, nls_timestamp_tz_format_,
    sql_mode_);

bool ObAlterIndexArg::is_valid() const
{
  // store_columns_ can be empty
  return (ObIndexArg::is_valid() && (0 == index_visibility_ || 1 == index_visibility_));
}

DEF_TO_STRING(ObAlterIndexArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME(N_INDEX_ARG);
  J_COLON();
  pos += ObIndexArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(index_visibility));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterIndexArg, ObIndexArg), index_visibility_);

DEF_TO_STRING(ObDropIndexArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
      K_(index_name),
      K_(table_name),
      K_(database_name),
      K_(index_action_type),
      K_(to_recyclebin),
      K_(index_table_id));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER((ObDropIndexArg, ObIndexArg), tenant_id_, index_name_, table_name_, database_name_,
    index_action_type_, to_recyclebin_, index_table_id_);

DEF_TO_STRING(ObRebuildIndexArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(index_name), K_(table_name), K_(database_name), K_(index_action_type), K_(index_table_id));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER((ObRebuildIndexArg, ObIndexArg), create_mode_, index_table_id_);

bool ObRenameIndexArg::is_valid() const
{
  int bret = true;
  if (origin_index_name_.empty()) {
    bret = false;
    LOG_WARN("origin_index_name is empty", K_(origin_index_name));
  } else if (new_index_name_.empty()) {
    bret = false;
    LOG_WARN("new_index_name is empty", K_(origin_index_name));
  } else {
    bret = ObIndexArg::is_valid();
  }
  return bret;
}

DEF_TO_STRING(ObAlterIndexParallelArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME(N_INDEX_ARG);
  J_COLON();
  pos += ObIndexArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(new_parallel));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterIndexParallelArg, ObIndexArg), new_parallel_);

DEF_TO_STRING(ObRenameIndexArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME(N_INDEX_ARG);
  J_COLON();
  pos += ObIndexArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(origin_index_name), K_(new_index_name));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObRenameIndexArg, ObIndexArg), origin_index_name_, new_index_name_);

bool ObCreateForeignKeyArg::is_valid() const
{
  return ObIndexArg::is_valid() && !parent_table_.empty() && child_columns_.count() > 0 &&
         parent_columns_.count() > 0 && child_columns_.count() == parent_columns_.count() &&
         ACTION_INVALID < update_action_ && update_action_ < ACTION_MAX && ACTION_INVALID < delete_action_ &&
         delete_action_ < ACTION_MAX;
}

DEF_TO_STRING(ObCreateForeignKeyArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME(N_FOREIGN_KEY_ARG);
  J_COLON();
  pos += ObIndexArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(parent_database),
      K_(parent_table),
      K_(child_columns),
      K_(parent_columns),
      K_(update_action),
      K_(delete_action),
      K_(foreign_key_name),
      K_(enable_flag),
      K_(is_modify_enable_flag),
      K_(ref_cst_type),
      K_(ref_cst_id),
      K_(validate_flag),
      K_(is_modify_validate_flag),
      K_(rely_flag),
      K_(is_modify_rely_flag),
      K_(is_modify_fk_state));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateForeignKeyArg, ObIndexArg), parent_database_, parent_table_, child_columns_,
    parent_columns_, update_action_, delete_action_, foreign_key_name_, enable_flag_, is_modify_enable_flag_,
    ref_cst_type_, ref_cst_id_, validate_flag_, is_modify_validate_flag_, rely_flag_, is_modify_rely_flag_,
    is_modify_fk_state_);

bool ObDropForeignKeyArg::is_valid() const
{
  return ObIndexArg::is_valid() && !foreign_key_name_.empty();
}

DEF_TO_STRING(ObDropForeignKeyArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME(N_FOREIGN_KEY_ARG);
  J_COLON();
  pos += ObIndexArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(foreign_key_name));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropForeignKeyArg, ObIndexArg), foreign_key_name_);

bool ObFlashBackTableFromRecyclebinArg::is_valid() const
{
  int bret = true;
  if (OB_INVALID_ID == tenant_id_) {
    bret = false;
    LOG_WARN("tenant_id is invalid", K_(tenant_id));
  } else if (origin_table_name_.empty()) {
    bret = false;
    LOG_WARN("origin_table_name is empty", K_(origin_table_name));
  } else if ((new_db_name_.empty() && !new_table_name_.empty()) || (!new_db_name_.empty() && new_table_name_.empty())) {
    bret = false;
    LOG_WARN("new_db_name or new_table_name is invalid", K_(new_db_name), K_(new_table_name));
  }
  return bret;
}

OB_SERIALIZE_MEMBER((ObFlashBackTableFromRecyclebinArg, ObDDLArg), tenant_id_, origin_table_name_, new_db_name_,
    new_table_name_, origin_db_name_);

bool ObFlashBackIndexArg::is_valid() const
{
  int bret = true;
  if (OB_INVALID_ID == tenant_id_) {
    bret = false;
    LOG_WARN("tenant_id is invalid", K_(tenant_id));
  } else if (origin_table_name_.empty()) {
    bret = false;
    LOG_WARN("origin_table_name is empty", K_(origin_table_name));
  } else if ((new_db_name_.empty() && !new_table_name_.empty()) || (!new_db_name_.empty() && new_table_name_.empty())) {
    bret = false;
    LOG_WARN("new_db_name or new_table_name is invalid", K_(new_db_name), K_(new_table_name));
  }
  return bret;
}

OB_SERIALIZE_MEMBER((ObFlashBackIndexArg, ObDDLArg), tenant_id_, origin_table_name_, new_db_name_, new_table_name_);

bool ObPurgeIndexArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !table_name_.empty();
}

OB_SERIALIZE_MEMBER((ObPurgeIndexArg, ObDDLArg), tenant_id_, table_name_);

bool ObFlashBackDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !origin_db_name_.empty();
}

OB_SERIALIZE_MEMBER((ObFlashBackDatabaseArg, ObDDLArg), tenant_id_, origin_db_name_, new_db_name_);

bool ObFlashBackTenantArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER((ObFlashBackTenantArg, ObDDLArg), tenant_id_, origin_tenant_name_, new_tenant_name_);

bool ObPurgeTableArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !table_name_.empty();
}

OB_SERIALIZE_MEMBER((ObPurgeTableArg, ObDDLArg), tenant_id_, table_name_);

bool ObPurgeDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !db_name_.empty();
}

OB_SERIALIZE_MEMBER((ObPurgeDatabaseArg, ObDDLArg), tenant_id_, db_name_);

bool ObPurgeTenantArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !tenant_name_.empty();
}

OB_SERIALIZE_MEMBER((ObPurgeTenantArg, ObDDLArg), tenant_id_, tenant_name_);

bool ObPurgeRecycleBinArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ || 0 == purge_num_ || 0 == expire_time_;
}

int ObPurgeRecycleBinArg::assign(const ObPurgeRecycleBinArg& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  purge_num_ = other.purge_num_;
  expire_time_ = other.expire_time_;
  auto_purge_ = other.auto_purge_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObPurgeRecycleBinArg, ObDDLArg), tenant_id_, purge_num_, expire_time_, auto_purge_);

OB_SERIALIZE_MEMBER((ObProfileDDLArg, ObDDLArg), schema_, ddl_type_, is_cascade_);

bool ObReachPartitionLimitArg::is_valid() const
{
  return batch_cnt_ > 0 && OB_INVALID_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER(ObReachPartitionLimitArg, batch_cnt_, tenant_id_, is_pg_arg_);
OB_SERIALIZE_MEMBER(ObCheckFrozenVersionArg, frozen_version_);
OB_SERIALIZE_MEMBER(ObGetMinSSTableSchemaVersionArg, tenant_id_arg_list_);
OB_SERIALIZE_MEMBER(ObGetMinSSTableSchemaVersionRes, ret_list_);

bool ObCreatePartitionArg::is_valid() const
{
  bool check_member_list = false;
  check_member_list = !ignore_member_list_ && restore_ <= REPLICA_LOGICAL_RESTORE_DATA;
  return !zone_.is_empty() && (pg_key_.is_valid() || partition_key_.is_valid()) && schema_version_ >= 0 &&
         memstore_version_ > 0 && replica_num_ > 0 &&
         ((!check_member_list && 0 == member_list_.get_member_number()) || check_member_list) &&
         ((!is_binding() && member_list_.get_member_number() > 0) || is_binding() || !check_member_list) &&
         ((!is_binding() && leader_.is_valid()) || is_binding()) && lease_start_ > 0 && logonly_replica_num_ >= 0 &&
         backup_replica_num_ >= 0 && readonly_replica_num_ >= 0 &&
         ((!is_binding() && common::ObReplicaTypeCheck::is_replica_type_valid(replica_type_)) ||
             is_binding())  // No need to specify the copy type when binding
         && last_submit_timestamp_ >= 0 &&
         ((!is_create_pg() && table_schemas_.count() > 0)        // create pg partition or create partition
             || (is_create_pg() && table_schemas_.count() == 0)  // create pg
             || is_standby_restore()) &&
         (frozen_timestamp_ > 0) && non_paxos_replica_num_ >= 0 && OB_INVALID_ID != last_replay_log_id_ &&
         replica_property_.is_valid();
}

int ObCreatePartitionArg::deep_copy(const ObCreatePartitionArg& arg)
{
  int ret = OB_SUCCESS;

  if (this != &arg) {
    zone_ = arg.zone_;
    pg_key_ = arg.pg_key_;
    partition_key_ = arg.partition_key_;
    schema_version_ = arg.schema_version_;
    memstore_version_ = arg.memstore_version_;
    replica_num_ = arg.replica_num_;
    leader_ = arg.leader_;
    lease_start_ = arg.lease_start_;
    logonly_replica_num_ = arg.logonly_replica_num_;
    backup_replica_num_ = arg.backup_replica_num_;
    readonly_replica_num_ = arg.readonly_replica_num_;
    replica_type_ = arg.replica_type_;
    last_submit_timestamp_ = arg.last_submit_timestamp_;
    restore_ = arg.restore_;
    source_partition_key_ = arg.source_partition_key_;
    frozen_timestamp_ = arg.frozen_timestamp_;
    non_paxos_replica_num_ = arg.non_paxos_replica_num_;
    last_replay_log_id_ = arg.last_replay_log_id_;
    replica_property_ = arg.replica_property_;
    ignore_member_list_ = arg.ignore_member_list_;
    if (OB_FAIL(member_list_.deep_copy(arg.member_list_))) {
      STORAGE_LOG(WARN, "member list deep copy error", K(arg.member_list_));
    } else if (OB_FAIL(table_schemas_.assign(arg.table_schemas_))) {
      STORAGE_LOG(WARN, "table schema assign error", K(ret), K(arg.table_schemas_));
    } else if (OB_FAIL(copy_assign(replica_property_, arg.replica_property_))) {
      STORAGE_LOG(WARN, "fail to copy replica property", K(ret));
    } else if (OB_FAIL(split_info_.assign(arg.split_info_))) {
      STORAGE_LOG(WARN, "fail to assign split info", K(ret));
    }
  }

  return ret;
}

int ObCreatePartitionArg::assign(const ObCreatePartitionArg& arg)
{
  return deep_copy(arg);
}

int ObCreatePartitionArg::check_need_create_sstable(bool& need_create_sstable) const
{
  int ret = OB_SUCCESS;
  need_create_sstable = true;

  if (1 == table_schemas_.count() && table_schemas_.at(0).is_global_index_table() &&
      table_schemas_.at(0).get_index_status() == ObIndexStatus::INDEX_STATUS_UNAVAILABLE) {
    need_create_sstable = false;
    SHARE_LOG(INFO, "unavailable global index, no need create sstable", K(*this));
  } else if (split_info_.is_valid()) {
    need_create_sstable = false;
    SHARE_LOG(INFO, "split dest partition, no need create sstable", K(*this));
  } else if (REPLICA_NOT_RESTORE != restore_ && REPLICA_RESTORE_LOG != restore_ &&
             REPLICA_RESTORE_ARCHIVE_DATA != restore_) {
    need_create_sstable = false;
    SHARE_LOG(INFO, "restore partition, no need create sstable", K(*this));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(replica_type_)) {
    need_create_sstable = false;
    SHARE_LOG(INFO, "replica without ssstore, no need create sstable", K(*this));
  } else if (is_standby_restore()) {
    need_create_sstable = false;
    SHARE_LOG(INFO, "replica is standby restore, no need create sstable", K(*this));
  }
  return ret;
}

bool ObCreatePartitionStorageArg::is_valid() const
{
  return rgkey_.is_valid() && partition_key_.is_valid() && schema_version_ >= 0 && memstore_version_ > 0 &&
         last_submit_timestamp_ >= 0 && table_schemas_.count() > 0;
}

bool ObCreatePartitionBatchArg::is_valid() const
{
  bool bool_ret = (0 < args_.count());
  FOREACH_CNT_X(a, args_, bool_ret)
  {
    if (OB_ISNULL(a)) {
      bool_ret = false;
      SHARE_LOG(WARN, "a is NULL");
    } else {
      bool_ret &= a->is_valid();
    }
  }
  return bool_ret;
}

int ObCreatePartitionBatchArg::assign(const ObCreatePartitionBatchArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(args_, other.args_))) {
    SHARE_LOG(WARN, "failed to assign args_", K(ret));
  }
  return ret;
}

DEF_TO_STRING(ObCreatePartitionArg)
{
  int64_t pos = 0;
  J_KV(K_(zone),
      K_(partition_key),
      K_(pg_key),
      K_(schema_version),
      K_(memstore_version),
      K_(replica_num),
      K_(member_list),
      K_(leader),
      K_(lease_start),
      K_(logonly_replica_num),
      K_(backup_replica_num),
      K_(readonly_replica_num),
      K_(replica_type),
      K_(last_submit_timestamp),
      K_(restore),
      K_(frozen_timestamp),
      K_(non_paxos_replica_num),
      K_(last_replay_log_id),
      K_(replica_property),
      K_(ignore_member_list),
      K_(split_info),
      K_(table_schemas));
  return pos;
}

DEF_TO_STRING(ObCreatePartitionStorageArg)
{
  int64_t pos = 0;
  J_KV(K_(rgkey),
      K_(partition_key),
      K_(schema_version),
      K_(memstore_version),
      K_(last_submit_timestamp),
      K_(restore),
      K_(table_schemas));
  return pos;
}

void ObCreatePartitionBatchRes::reset()
{
  ret_list_.reset();
  timestamp_ = OB_INVALID_TIMESTAMP;
}

void ObCreatePartitionBatchRes::reuse()
{
  ret_list_.reuse();
  timestamp_ = OB_INVALID_TIMESTAMP;
}

int ObCreatePartitionBatchRes::assign(const ObCreatePartitionBatchRes& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(ret_list_, other.ret_list_))) {
    SHARE_LOG(WARN, "failed to assign ret_list_", K(ret));
  } else {
    timestamp_ = other.timestamp_;
  }
  return ret;
}

DEF_TO_STRING(ObCreatePartitionBatchRes)
{
  int64_t pos = 0;
  J_KV(K_(ret_list), K_(timestamp));
  return pos;
}

DEF_TO_STRING(ObCreatePartitionBatchArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(args));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObCreatePartitionArg, zone_, partition_key_, memstore_version_, replica_num_, member_list_, leader_,
    lease_start_, logonly_replica_num_, backup_replica_num_, readonly_replica_num_, replica_type_,
    last_submit_timestamp_, schema_version_, restore_, table_schemas_, source_partition_key_, frozen_timestamp_,
    non_paxos_replica_num_, last_replay_log_id_, pg_key_, replica_property_, split_info_, ignore_member_list_);

OB_SERIALIZE_MEMBER(ObCreatePartitionStorageArg, rgkey_, partition_key_, memstore_version_, last_submit_timestamp_,
    schema_version_, restore_, table_schemas_);

OB_SERIALIZE_MEMBER(ObCreatePartitionBatchArg, args_);

OB_SERIALIZE_MEMBER(ObCreatePartitionBatchRes, ret_list_, timestamp_);

bool ObSetMemberListArg::is_valid() const
{
  bool bret = false;
  bret = key_.is_valid() && member_list_.is_valid();

  if (bret && CLUSTER_VERSION_2250 <= GET_MIN_CLUSTER_VERSION()) {
    bret = quorum_ >= member_list_.get_member_number();
  }
  if (bret && CLUSTER_VERSION_2260 <= GET_MIN_CLUSTER_VERSION()) {
    bret = lease_start_ > 0 && leader_.is_valid();
  }
  return bret;
}
void ObSetMemberListArg::reset()
{
  key_.reset();
  member_list_.reset();
  quorum_ = 0;
  lease_start_ = 0;
  leader_.reset();
}
int ObSetMemberListArg::assign(const ObSetMemberListArg& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(member_list_.deep_copy(other.member_list_))) {
      LOG_WARN("failed to copy member list", K(ret));
    } else {
      key_ = other.key_;
      quorum_ = other.quorum_;
      lease_start_ = other.lease_start_;
      leader_ = other.leader_;
    }
  }
  return ret;
}
int ObSetMemberListArg::init(const int64_t table_id, const int64_t partition_id, const int64_t partition_cnt,
    const common::ObMemberList& member_list, const int64_t quorum, const int64_t lease_start,
    const common::ObAddr& leader)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(member_list_.deep_copy(member_list))) {
    LOG_WARN("failed to assign member list", K(ret), K(member_list));
  } else if (OB_FAIL(key_.init(table_id, partition_id, partition_cnt))) {
    LOG_WARN("failed to init partition key", K(ret), K(table_id), K(partition_id), K(partition_cnt));
  } else {
    quorum_ = quorum;
    lease_start_ = lease_start;
    leader_ = leader;
  }
  return ret;
}

DEF_TO_STRING(ObSetMemberListArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(key), K_(member_list), K_(quorum), K_(lease_start), K_(leader));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObSetMemberListArg, key_, member_list_, quorum_, lease_start_, leader_);

void ObSetMemberListBatchArg::reset()
{
  args_.reset();
  timestamp_ = common::OB_INVALID_TIMESTAMP;
}

bool ObSetMemberListBatchArg::is_valid() const
{
  return 0 < args_.count();
}

int ObSetMemberListBatchArg::assign(const ObSetMemberListBatchArg& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(args_.assign(other.args_))) {
      LOG_WARN("failed to copy member list", K(ret));
    } else {
      timestamp_ = other.timestamp_;
    }
  }
  return ret;
}

int ObSetMemberListBatchArg::add_arg(const ObPartitionKey& key, const common::ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  ObSetMemberListArg arg;
  if (OB_FAIL(arg.init(key.get_table_id(),
          key.get_partition_id(),
          0 /*partition_cnt*/,
          member_list,
          member_list.get_member_number()))) {
    LOG_WARN("fail to init arg", K(ret), K(arg), K(member_list));
  } else if (OB_FAIL(args_.push_back(arg))) {
    LOG_WARN("fail to push back arg", K(ret), K(arg));
  }
  return ret;
}
DEF_TO_STRING(ObSetMemberListBatchArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(args), K_(timestamp));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObSetMemberListBatchArg, args_, timestamp_);

OB_SERIALIZE_MEMBER(ObCheckUniqueIndexRequestArg, pkey_, index_id_, schema_version_);

bool ObCheckUniqueIndexRequestArg::is_valid() const
{
  return pkey_.is_valid() && OB_INVALID_ID != index_id_ && schema_version_ >= 0;
}

void ObCheckUniqueIndexRequestArg::reset()
{
  pkey_.reset();
  index_id_ = OB_INVALID_ID;
  schema_version_ = 0;
}

OB_SERIALIZE_MEMBER(ObCheckUniqueIndexResponseArg, pkey_, ret_code_, is_valid_);

bool ObCheckUniqueIndexResponseArg::is_valid() const
{
  return pkey_.is_valid() && OB_INVALID != index_id_;
}

void ObCheckUniqueIndexResponseArg::reset()
{
  pkey_.reset();
  index_id_ = OB_INVALID_ID;
  ret_code_ = OB_SUCCESS;
  is_valid_ = false;
}

OB_SERIALIZE_MEMBER(
    ObCalcColumnChecksumRequestArg, pkey_, index_id_, schema_version_, execution_id_, snapshot_version_);

bool ObCalcColumnChecksumRequestArg::is_valid() const
{
  return pkey_.is_valid() && OB_INVALID_ID != index_id_ && OB_INVALID_VERSION != schema_version_ &&
         OB_INVALID_ID != execution_id_ && OB_INVALID_VERSION != snapshot_version_;
}

void ObCalcColumnChecksumRequestArg::reset()
{
  pkey_.reset();
  index_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  execution_id_ = OB_INVALID_ID;
  snapshot_version_ = OB_INVALID_VERSION;
}

OB_SERIALIZE_MEMBER(ObCalcColumnChecksumResponseArg, pkey_, index_id_, ret_code_);

bool ObCalcColumnChecksumResponseArg::is_valid() const
{
  return pkey_.is_valid() && OB_INVALID_ID != index_id_;
}

void ObCalcColumnChecksumResponseArg::reset()
{
  pkey_.reset();
  index_id_ = OB_INVALID_ID;
  ret_code_ = OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObCheckSingleReplicaMajorSSTableExistArg, pkey_, index_id_);

bool ObCheckSingleReplicaMajorSSTableExistArg::is_valid() const
{
  return pkey_.is_valid() && OB_INVALID_ID != index_id_;
}

void ObCheckSingleReplicaMajorSSTableExistArg::reset()
{
  pkey_.reset();
  index_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCheckSingleReplicaMajorSSTableExistResult, timestamp_);

void ObCheckSingleReplicaMajorSSTableExistResult::reset()
{
  timestamp_ = 0;
}

OB_SERIALIZE_MEMBER(ObCheckAllReplicaMajorSSTableExistArg, pkey_, index_id_);

bool ObCheckAllReplicaMajorSSTableExistArg::is_valid() const
{
  return pkey_.is_valid() && OB_INVALID_ID != index_id_;
}

void ObCheckAllReplicaMajorSSTableExistArg::reset()
{
  pkey_.reset();
  index_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCheckAllReplicaMajorSSTableExistResult, max_timestamp_);

void ObCheckAllReplicaMajorSSTableExistResult::reset()
{
  max_timestamp_ = 0;
}

bool ObMajorFreezeArg::is_valid() const
{
  return frozen_version_ > 0 && schema_version_ > 0 && frozen_timestamp_ > 0;
}

DEF_TO_STRING(ObMajorFreezeArg)
{
  int64_t pos = 0;
  J_KV(K_(frozen_version), K_(schema_version), K_(frozen_timestamp));
  return pos;
}

OB_SERIALIZE_MEMBER(ObAddReplicaBatchRes, res_array_);

bool ObAddReplicaBatchRes::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObRebuildReplicaBatchRes, res_array_);

bool ObRebuildReplicaBatchRes::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObCopySSTableBatchRes, res_array_, type_);

bool ObCopySSTableBatchRes::is_valid() const
{
  bool is_valid = OB_COPY_SSTABLE_TYPE_INVALID != type_;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObMigrateReplicaBatchRes, res_array_);

bool ObMigrateReplicaBatchRes::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObStandbyCutDataBatchTaskRes, res_array_);

bool ObStandbyCutDataBatchTaskRes::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObChangeReplicaBatchRes, res_array_);

bool ObChangeReplicaBatchRes::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObBackupBatchRes, res_array_);

bool ObBackupBatchRes::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObValidateBatchRes, res_array_);

int ObValidateBatchRes::assign(const ObValidateBatchRes& res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(res_array_, res.res_array_))) {
    SHARE_LOG(WARN, "failed to assign res_array_", K(ret));
  }
  return ret;
}

bool ObValidateBatchRes::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

// ---structs for partition batch online/offline---
OB_SERIALIZE_MEMBER(ObAddReplicaBatchArg, arg_array_, timeout_ts_, task_id_);

bool ObAddReplicaBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObRemoveNonPaxosReplicaBatchArg, arg_array_, timeout_ts_, task_id_);

bool ObRemoveNonPaxosReplicaBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObRemoveNonPaxosReplicaBatchResult, return_array_);

OB_SERIALIZE_MEMBER(
    ObStandbyCutDataBatchTaskArg, arg_array_, timeout_ts_, trace_id_, flashback_ts_, switchover_epoch_, fo_trace_id_);
bool ObStandbyCutDataBatchTaskArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}
int ObStandbyCutDataBatchTaskArg::init(const int64_t timeout, const share::ObTaskId& task_id,
    const common::ObCurTraceId::TraceId& fo_trace_id, const int64_t flashback_ts, const int64_t switchover_epoch)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == timeout || 0 >= flashback_ts || 0 >= switchover_epoch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(timeout), K(flashback_ts), K(switchover_epoch));
  } else {
    timeout_ts_ = timeout + ObTimeUtility::current_time();
    trace_id_ = task_id;
    fo_trace_id_ = fo_trace_id;
    flashback_ts_ = flashback_ts;
    switchover_epoch_ = switchover_epoch;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMigrateReplicaBatchArg, arg_array_, timeout_ts_, task_id_);

bool ObMigrateReplicaBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObChangeReplicaBatchArg, arg_array_, timeout_ts_, task_id_);

bool ObChangeReplicaArg::is_valid() const
{
  bool bret = false;
  bret = key_.is_valid() && src_.is_valid() && dst_.is_valid() && is_replica_op_priority_valid(priority_);
  if (bret && !IS_CLUSTER_VERSION_BEFORE_2200) {
    bret = (OB_INVALID_VERSION != switch_epoch_);
  }
  if (bret) {
    if (GCTX.is_primary_cluster() || ObMultiClusterUtil::is_cluster_private_table(key_.get_table_id())) {
      bret = quorum_ > 0;
    }
  }
  return bret;
}

bool ObChangeReplicaBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObRebuildReplicaBatchArg, arg_array_, timeout_ts_, task_id_);

bool ObRebuildReplicaBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObCopySSTableBatchArg, arg_array_, timeout_ts_, task_id_, type_);

bool ObCopySSTableBatchArg::is_valid() const
{
  bool is_valid = OB_COPY_SSTABLE_TYPE_INVALID != type_;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObServerCopyLocalIndexSSTableArg, data_src_, dst_, pkey_, index_table_id_, cluster_id_, data_size_);

bool ObServerCopyLocalIndexSSTableArg::is_valid() const
{
  return data_src_.is_valid() && dst_.is_valid() && pkey_.is_valid() && OB_INVALID_ID != index_table_id_ &&
         cluster_id_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObBackupBatchArg, arg_array_, timeout_ts_, task_id_);

bool ObBackupBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObValidateBatchArg, arg_array_, timeout_ts_, task_id_);

int ObValidateBatchArg::assign(const ObValidateBatchArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(arg_array_, arg.arg_array_))) {
    SHARE_LOG(WARN, "failed to assign arg_array_", K(ret));
  } else {
    timeout_ts_ = arg.timeout_ts_;
    task_id_.set(arg.task_id_);
  }
  return ret;
}

bool ObValidateBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObPhyRestoreReplicaArg, key_, src_, dst_, task_id_);
ObPhyRestoreReplicaArg::ObPhyRestoreReplicaArg()
    : key_(), src_(), dst_(), task_id_(), priority_(common::ObReplicaOpPriority::PRIO_LOW)
{}
bool ObPhyRestoreReplicaArg::is_valid() const
{
  return key_.is_valid() && src_.is_valid() && dst_.is_valid() && is_replica_op_priority_valid(priority_);
}

OB_SERIALIZE_MEMBER(ObPhyRestoreReplicaRes, key_, src_, dst_, result_);
ObPhyRestoreReplicaRes::ObPhyRestoreReplicaRes() : key_(), src_(), dst_(), result_(0)
{}
bool ObPhyRestoreReplicaRes::is_valid() const
{
  return key_.is_valid() && src_.is_valid() && dst_.is_valid();
}

OB_SERIALIZE_MEMBER(ObAuthReplicaMovingkArg, pg_key_, addr_, file_id_, type_);

bool ObAuthReplicaMovingkArg::is_valid() const
{
  return pg_key_.is_valid() && addr_.is_valid() && file_id_ > 0 && type_ > REPLICA_MOVING_TYPE_INVALID &&
         type_ < REPLICA_MOVING_TYPE_MAX;
}

int ObValidateArg::assign(const ObValidateArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(physical_validate_arg_.assign(arg.physical_validate_arg_))) {
    SHARE_LOG(WARN, "failed to assign physical_validate_args_", K(ret));
  } else {
    trace_id_.set(arg.trace_id_);
    dst_ = arg.dst_;
    priority_ = arg.priority_;
  }
  return ret;
}

int ObValidateRes::assign(const ObValidateRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(validate_arg_.assign(arg.validate_arg_))) {
    SHARE_LOG(WARN, "failed to assign validate_args_", K(ret));
  } else {
    key_ = arg.key_;
    dst_ = arg.dst_;
    result_ = arg.result_;
  }
  return ret;
}

//----Structs for partition online/offline----

OB_SERIALIZE_MEMBER(ObAddReplicaArg, key_, src_, dst_, quorum_, reserved_modify_quorum_type_, task_id_, cluster_id_,
    skip_change_member_list_, switch_epoch_);
OB_SERIALIZE_MEMBER(ObAddReplicaRes, key_, src_, dst_, data_src_, quorum_, result_);
OB_SERIALIZE_MEMBER(ObRemoveNonPaxosReplicaArg, key_, dst_, task_id_, skip_change_member_list_, switch_epoch_);
OB_SERIALIZE_MEMBER(ObMigrateReplicaArg, key_, src_, dst_, data_source_, quorum_, task_id_, skip_change_member_list_,
    switch_epoch_, migrate_mode_);
OB_SERIALIZE_MEMBER(ObMigrateReplicaRes, key_, src_, dst_, data_src_, result_);
OB_SERIALIZE_MEMBER(ObChangeReplicaArg, key_, src_, dst_, quorum_, task_id_, skip_change_member_list_, switch_epoch_);
OB_SERIALIZE_MEMBER(ObChangeReplicaRes, key_, src_, dst_, data_src_, quorum_, result_);
OB_SERIALIZE_MEMBER(ObRebuildReplicaArg, key_, src_, dst_, task_id_, skip_change_member_list_, switch_epoch_);
OB_SERIALIZE_MEMBER(ObRebuildReplicaRes, key_, src_, dst_, data_src_, result_);
OB_SERIALIZE_MEMBER(ObRestoreReplicaArg, key_, src_, dst_, task_id_, skip_change_member_list_, switch_epoch_);
OB_SERIALIZE_MEMBER(ObRestoreReplicaRes, key_, src_, dst_, result_);
OB_SERIALIZE_MEMBER(ObCopySSTableArg, key_, src_, dst_, task_id_, type_, index_table_id_, cluster_id_,
    skip_change_member_list_, switch_epoch_);
OB_SERIALIZE_MEMBER(ObCopySSTableRes, key_, src_, dst_, data_src_, type_, index_table_id_, result_);
OB_SERIALIZE_MEMBER(ObBackupArg, key_, src_, dst_, physical_backup_arg_, task_id_, cluster_id_,
    skip_change_member_list_, switch_epoch_);
OB_SERIALIZE_MEMBER(ObBackupRes, key_, src_, dst_, data_src_, physical_backup_arg_, result_);
OB_SERIALIZE_MEMBER(ObValidateArg, trace_id_, dst_, physical_validate_arg_, priority_);
OB_SERIALIZE_MEMBER(ObValidateRes, key_, dst_, validate_arg_, result_);
OB_SERIALIZE_MEMBER(ObPhysicalFlashbackResultArg, min_version_, max_version_, enable_result_);
OB_SERIALIZE_MEMBER(ObCheckPhysicalFlashbackArg, merged_version_, flashback_scn_);
OB_SERIALIZE_MEMBER(ObStandbyCutDataTaskArg, pkey_, dst_);
OB_SERIALIZE_MEMBER(ObStandbyCutDataTaskRes, key_, dst_, result_);

//----End structs for partition online/offline----

OB_SERIALIZE_MEMBER(ObMajorFreezeArg, frozen_version_, schema_version_, frozen_timestamp_);

DEF_TO_STRING(ObSetReplicaNumArg)
{
  int64_t pos = 0;
  J_KV(K_(partition_key), K_(replica_num));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSetReplicaNumArg, partition_key_, replica_num_);

DEF_TO_STRING(ObSetParentArg)
{
  int64_t pos = 0;
  J_KV(K_(partition_key), K_(parent_addr));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSetParentArg, partition_key_, parent_addr_);

void ObSwitchSchemaArg::reset()
{
  schema_info_.reset();
  force_refresh_ = false;
}

DEF_TO_STRING(ObSwitchSchemaArg)
{
  int64_t pos = 0;
  J_KV(K_(schema_info), K_(force_refresh));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSwitchSchemaArg, schema_info_, force_refresh_);

DEF_TO_STRING(ObSwitchLeaderArg)
{
  int64_t pos = 0;
  J_KV(K_(partition_key), K_(leader_addr));
  return pos;
}

int ObCheckSchemaVersionElapsedArg::build(rootserver::ObGlobalIndexTask* task, common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == task || !pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task), K(pkey));
  } else {
    pkey_ = pkey;
    schema_version_ = task->schema_version_;
  }
  return ret;
}

int ObCheckCtxCreateTimestampElapsedArg::build(rootserver::ObGlobalIndexTask* task, common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == task || !pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task), K(pkey));
  } else {
    pkey_ = pkey;
    sstable_exist_ts_ = task->major_sstable_exist_reply_ts_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSwitchLeaderArg, partition_key_, leader_addr_);

OB_SERIALIZE_MEMBER(ObCheckSchemaVersionElapsedArg, pkey_, schema_version_);

OB_SERIALIZE_MEMBER(ObCheckCtxCreateTimestampElapsedArg, pkey_, sstable_exist_ts_);

OB_SERIALIZE_MEMBER(ObCheckSchemaVersionElapsedResult, snapshot_);

OB_SERIALIZE_MEMBER(ObCheckCtxCreateTimestampElapsedResult, snapshot_);

OB_SERIALIZE_MEMBER(ObGetLeaderCandidatesArg, partitions_);

OB_SERIALIZE_MEMBER(ObGetLeaderCandidatesV2Arg, partitions_, prep_candidates_);

OB_SERIALIZE_MEMBER(CandidateStatus, candidate_status_);

OB_SERIALIZE_MEMBER(ObGetLeaderCandidatesResult, candidates_, candidate_status_array_);

//----Structs for managing privileges----
OB_SERIALIZE_MEMBER(ObAccountArg, user_name_, host_name_, is_role_);

bool ObCreateUserArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && user_infos_.count() > 0;
}

int ObCreateUserArg::assign(const ObCreateUserArg& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  if_not_exist_ = other.if_not_exist_;
  creator_id_ = other.creator_id_;
  primary_zone_ = other.primary_zone_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(user_infos_.assign(other.user_infos_))) {
    LOG_WARN("failed to assign user info", KR(ret), K(other));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateUserArg, ObDDLArg), tenant_id_, user_infos_, if_not_exist_, creator_id_, primary_zone_);

bool ObDropUserArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && users_.count() > 0 && hosts_.count() == users_.count();
}

OB_DEF_SERIALIZE(ObDropUserArg)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, users_, hosts_, is_role_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDropUserArg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, users_, hosts_);

  // compatibility for old version
  if (OB_SUCC(ret) && users_.count() > 0 && hosts_.empty()) {
    const ObString TMP_DEFAULT_HOST_NAME(OB_DEFAULT_HOST_NAME);
    for (int64_t i = 0; i < users_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(hosts_.push_back(TMP_DEFAULT_HOST_NAME))) {
        LOG_WARN("fail to push_back DEFAULT_HOST_NAME", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, is_role_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDropUserArg)
{
  int64_t len = ObDDLArg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, users_, hosts_, is_role_);
  return len;
}

bool ObRenameUserArg::is_valid() const
{
  return (OB_INVALID_ID != tenant_id_ && old_users_.count() > 0 && old_users_.count() == new_users_.count() &&
          old_hosts_.count() == new_hosts_.count() && old_users_.count() == old_hosts_.count());
}

OB_DEF_SERIALIZE(ObRenameUserArg)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, old_users_, new_users_, old_hosts_, new_hosts_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRenameUserArg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, old_users_, new_users_, old_hosts_, new_hosts_);

  // compatibility for old version
  if (OB_SUCC(ret) && old_users_.count() > 0 && new_users_.count() == old_users_.count() &&
      (old_hosts_.empty() || new_hosts_.empty())) {
    if (old_hosts_.empty() != new_hosts_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old_hosts and new_hosts should have same count", K_(old_hosts), K_(new_hosts), K(ret));
    } else {
      const ObString TMP_DEFAULT_HOST_NAME(OB_DEFAULT_HOST_NAME);
      for (int64_t i = 0; i < old_users_.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(old_hosts_.push_back(TMP_DEFAULT_HOST_NAME))) {
          LOG_WARN("fail to push_back DEFAULT_HOST_NAME", K(ret));
        } else if (OB_FAIL(new_hosts_.push_back(TMP_DEFAULT_HOST_NAME))) {
          LOG_WARN("fail to push_back DEFAULT_HOST_NAME", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRenameUserArg)
{
  int64_t len = ObDDLArg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, old_users_, new_users_, old_hosts_, new_hosts_);
  return len;
}

bool ObSetPasswdArg::is_valid() const
{
  // user_name_ and passwd_ can be empty
  return OB_INVALID_ID != tenant_id_;
}

OB_DEF_SERIALIZE(ObSetPasswdArg)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, user_, passwd_, host_, ssl_type_, ssl_cipher_, x509_issuer_, x509_subject_,
    modify_max_connections_, max_connections_per_hour_, max_user_connections_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSetPasswdArg)
{
  int ret = OB_SUCCESS;
  host_.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
  ssl_type_ = schema::ObSSLType::SSL_TYPE_NOT_SPECIFIED;
  ssl_cipher_.reset();
  x509_issuer_.reset();
  x509_subject_.reset();

  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, user_, passwd_, host_, ssl_type_, ssl_cipher_, x509_issuer_, x509_subject_,
    modify_max_connections_, max_connections_per_hour_, max_user_connections_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSetPasswdArg)
{
  int64_t len = ObDDLArg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, user_, passwd_, host_, ssl_type_, ssl_cipher_, x509_issuer_, x509_subject_,
    modify_max_connections_, max_connections_per_hour_, max_user_connections_);
  return len;
}

bool ObLockUserArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && users_.count() > 0 && users_.count() == hosts_.count();
}

OB_DEF_SERIALIZE(ObLockUserArg)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, users_, locked_, hosts_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLockUserArg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, users_, locked_, hosts_);

  // compatibility for old version
  if (OB_SUCC(ret) && users_.count() > 0 && hosts_.empty()) {
    const ObString TMP_DEFAULT_HOST_NAME(OB_DEFAULT_HOST_NAME);
    for (int64_t i = 0; i < users_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(hosts_.push_back(TMP_DEFAULT_HOST_NAME))) {
        LOG_WARN("fail to push_back DEFAULT_HOST_NAME", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLockUserArg)
{
  int64_t len = ObDDLArg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, users_, locked_, hosts_);
  return len;
}

int ObAlterUserProfileArg::assign(const ObAlterUserProfileArg& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  user_name_ = other.user_name_;
  host_name_ = other.host_name_;
  profile_name_ = other.profile_name_;
  user_id_ = other.user_id_;
  default_role_flag_ = other.default_role_flag_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(role_id_array_.assign(other.role_id_array_))) {
    SHARE_LOG(WARN, "fail to assign role_id_array", K(ret));
  }
  return ret;
}

bool ObAlterUserProfileArg::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) &&
         ((user_name_.length() > 0 && host_name_.length() > 0) || is_valid_id(user_id_));
}

OB_SERIALIZE_MEMBER((ObAlterUserProfileArg, ObDDLArg), tenant_id_, user_name_, host_name_, profile_name_, user_id_,
    default_role_flag_, role_id_array_);

bool ObGrantArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      /* Oracle mode Different permission system
       * && priv_level_ > OB_PRIV_INVALID_LEVEL
      && priv_level_ < OB_PRIV_MAX_LEVEL
      && users_passwd_.count() > 0
      && users_passwd_.count() == hosts_.count() * 2
      */
      ;
}

bool ObGrantArg::is_allow_when_disable_ddl() const
{
  return is_inner_;
}

int ObGrantArg::assign(const ObGrantArg& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  priv_level_ = other.priv_level_;
  db_ = other.db_;
  table_ = other.table_;
  priv_set_ = other.priv_set_;
  need_create_user_ = other.need_create_user_;
  has_create_user_priv_ = other.has_create_user_priv_;
  option_ = other.option_;
  object_type_ = other.object_type_;
  object_id_ = other.object_id_;
  grantor_id_ = other.grantor_id_;
  is_inner_ = other.is_inner_;

  if (OB_FAIL(ObDDLArg::assign(other))) {
    SHARE_LOG(WARN, "fail to assign ddl arg", K(ret));
  } else if (OB_FAIL(users_passwd_.assign(other.users_passwd_))) {
    SHARE_LOG(WARN, "fail to assign users_passwd_", K(ret));
  } else if (OB_FAIL(hosts_.assign(other.hosts_))) {
    SHARE_LOG(WARN, "fail to assign hosts_", K(ret));
  } else if (OB_FAIL(roles_.assign(other.roles_))) {
    SHARE_LOG(WARN, "fail to assign roles_", K(ret));
  } else if (OB_FAIL(sys_priv_array_.assign(other.sys_priv_array_))) {
    SHARE_LOG(WARN, "fail to assign sys_priv_array_", K(ret));
  } else if (OB_FAIL(obj_priv_array_.assign(other.obj_priv_array_))) {
    SHARE_LOG(WARN, "fail to assign obj_priv_array_", K(ret));
  } else if (OB_FAIL(ins_col_ids_.assign(other.ins_col_ids_))) {
    SHARE_LOG(WARN, "fail to assign ins_col_ids_", K(ret));
  } else if (OB_FAIL(upd_col_ids_.assign(other.upd_col_ids_))) {
    SHARE_LOG(WARN, "fail to assign upd_col_ids_", K(ret));
  } else if (OB_FAIL(ref_col_ids_.assign(other.ref_col_ids_))) {
    SHARE_LOG(WARN, "fail to assign ref_col_ids_", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObGrantArg)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      priv_level_,
      db_,
      table_,
      priv_set_,
      users_passwd_,
      need_create_user_,
      has_create_user_priv_,
      hosts_,
      roles_,
      option_,
      sys_priv_array_,
      obj_priv_array_,
      object_type_,
      object_id_,
      ins_col_ids_,
      upd_col_ids_,
      ref_col_ids_,
      grantor_id_,
      remain_roles_,
      is_inner_);
  return ret;
}

OB_DEF_DESERIALIZE(ObGrantArg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      priv_level_,
      db_,
      table_,
      priv_set_,
      users_passwd_,
      need_create_user_,
      has_create_user_priv_,
      hosts_,
      roles_,
      option_,
      sys_priv_array_,
      obj_priv_array_,
      object_type_,
      object_id_,
      ins_col_ids_,
      upd_col_ids_,
      ref_col_ids_,
      grantor_id_,
      remain_roles_,
      is_inner_);

  // compatibility for old version
  if (OB_SUCC(ret) && users_passwd_.count() > 0 && hosts_.empty()) {
    const int64_t user_count = users_passwd_.count() / 2;
    const ObString TMP_DEFAULT_HOST_NAME(OB_DEFAULT_HOST_NAME);
    for (int64_t i = 0; i < user_count && OB_SUCC(ret); ++i) {
      if (OB_FAIL(hosts_.push_back(TMP_DEFAULT_HOST_NAME))) {
        LOG_WARN("fail to push_back DEFAULT_HOST_NAME", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObGrantArg)
{
  int64_t len = ObDDLArg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      priv_level_,
      db_,
      table_,
      priv_set_,
      users_passwd_,
      need_create_user_,
      has_create_user_priv_,
      hosts_,
      roles_,
      option_,
      sys_priv_array_,
      obj_priv_array_,
      object_type_,
      object_id_,
      ins_col_ids_,
      upd_col_ids_,
      ref_col_ids_,
      grantor_id_,
      remain_roles_,
      is_inner_);
  return len;
}

bool ObStandbyGrantArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != user_id_ && !db_.empty();
}

OB_SERIALIZE_MEMBER((ObStandbyGrantArg, ObDDLArg), tenant_id_, user_id_, db_, table_, priv_level_, priv_set_);

bool ObRevokeUserArg::is_valid() const
{
  // FIXME: Currently the role only supports revoke from user
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != user_id_;
}

OB_SERIALIZE_MEMBER((ObRevokeUserArg, ObDDLArg), tenant_id_, user_id_, priv_set_, revoke_all_, role_ids_);

bool ObRevokeDBArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != user_id_ && !db_.empty();
}

OB_SERIALIZE_MEMBER((ObRevokeDBArg, ObDDLArg), tenant_id_, user_id_, db_, priv_set_);

bool ObRevokeTableArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != user_id_ && !db_.empty() && !table_.empty();
}

OB_SERIALIZE_MEMBER((ObRevokeTableArg, ObDDLArg), tenant_id_, user_id_, db_, table_, priv_set_, grant_, obj_id_,
    obj_type_, grantor_id_, obj_priv_array_, revoke_all_ora_);

bool ObRevokeSysPrivArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != grantee_id_;
}

OB_SERIALIZE_MEMBER((ObRevokeSysPrivArg, ObDDLArg), tenant_id_, grantee_id_, sys_priv_array_);

//----End of structs for managing privileges----

bool ObAdminSwitchReplicaRoleArg::is_valid() const
{
  return partition_key_.is_valid() || server_.is_valid() || !zone_.is_empty();
}

OB_SERIALIZE_MEMBER(ObAdminSwitchReplicaRoleArg, role_, partition_key_, server_, zone_, tenant_name_);

bool ObAdminSwitchRSRoleArg::is_valid() const
{
  return server_.is_valid() || !zone_.is_empty();
}

OB_SERIALIZE_MEMBER(ObAdminSwitchRSRoleArg, role_, server_, zone_);

OB_SERIALIZE_MEMBER(ObCheckGtsReplicaStopZone, zone_);

OB_SERIALIZE_MEMBER(ObCheckGtsReplicaStopServer, servers_);

bool ObAdminDropReplicaArg::is_valid() const
{
  // zone can be empty
  return partition_key_.is_valid() && server_.is_valid() && create_timestamp_ >= 0;
}

OB_SERIALIZE_MEMBER(ObAdminDropReplicaArg, partition_key_, server_, zone_, create_timestamp_, force_cmd_);

bool ObAdminMigrateReplicaArg::is_valid() const
{
  return partition_key_.is_valid() && src_.is_valid() && dest_.is_valid() && src_ != dest_;
}

OB_SERIALIZE_MEMBER(ObAdminMigrateReplicaArg, is_copy_, partition_key_, src_, dest_, force_cmd_);

bool ObPhysicalRestoreTenantArg::is_valid() const
{
  return !tenant_name_.empty() && tenant_name_.length() < common::OB_MAX_TENANT_NAME_LENGTH_STORE &&
         !backup_tenant_name_.empty() && backup_tenant_name_.length() < common::OB_MAX_TENANT_NAME_LENGTH_STORE &&
         (!uri_.empty() || !multi_uri_.empty()) && uri_.length() < share::OB_MAX_BACKUP_DEST_LENGTH &&
         !restore_option_.empty() && restore_option_.length() < common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH &&
         restore_timestamp_ > 0;
}

OB_SERIALIZE_MEMBER((ObPhysicalRestoreTenantArg, ObCmdArg), tenant_name_, uri_, restore_option_, restore_timestamp_,
    backup_tenant_name_, passwd_array_, table_items_, multi_uri_);

ObPhysicalRestoreTenantArg::ObPhysicalRestoreTenantArg()
    : ObCmdArg(),
      tenant_name_(),
      uri_(),
      restore_option_(),
      restore_timestamp_(common::OB_INVALID_TIMESTAMP),
      backup_tenant_name_(),
      passwd_array_(),
      table_items_(),
      multi_uri_()
{}

int ObPhysicalRestoreTenantArg::assign(const ObPhysicalRestoreTenantArg& other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // skip
  } else if (OB_FAIL(table_items_.assign(other.table_items_))) {
    LOG_WARN("fail to assign table_items", KR(ret));
  } else {
    tenant_name_ = other.tenant_name_;
    uri_ = other.uri_;
    restore_option_ = other.restore_option_;
    restore_timestamp_ = other.restore_timestamp_;
    backup_tenant_name_ = other.backup_tenant_name_;
    passwd_array_ = other.passwd_array_;
  }
  return ret;
}

int ObPhysicalRestoreTenantArg::add_table_item(const ObTableItem& item)
{
  return table_items_.push_back(item);
}

OB_SERIALIZE_MEMBER((ObRestoreTenantArg, ObCmdArg), tenant_name_, oss_uri_);

bool ObRestorePartitionsArg::is_valid() const
{
  return (OB_CREATE_TABLE_MODE_PHYSICAL_RESTORE == mode_ || OB_CREATE_TABLE_MODE_RESTORE == mode_) && schema_id_ > 0;
}
OB_SERIALIZE_MEMBER(ObRestorePartitionsArg, schema_id_, mode_, partition_ids_, schema_version_);

OB_SERIALIZE_MEMBER(ObServerZoneArg, server_, zone_);

OB_SERIALIZE_MEMBER((ObRunJobArg, ObServerZoneArg), job_);

ObUpgradeJobArg::ObUpgradeJobArg() : action_(INVALID_ACTION), version_(common::OB_INVALID_VERSION)
{}
bool ObUpgradeJobArg::is_valid() const
{
  return INVALID_ACTION != action_ && version_ > 0;
}
int ObUpgradeJobArg::assign(const ObUpgradeJobArg& other)
{
  int ret = OB_SUCCESS;
  action_ = other.action_;
  version_ = other.version_;
  return ret;
}
OB_SERIALIZE_MEMBER(ObUpgradeJobArg, action_, version_);

OB_SERIALIZE_MEMBER(ObAdminFlushCacheArg, tenant_ids_, cache_type_, db_ids_, sql_id_, is_fine_grained_);

OB_SERIALIZE_MEMBER(ObFlushCacheArg, is_all_tenant_, tenant_id_, cache_type_, db_ids_, sql_id_, is_fine_grained_);

OB_SERIALIZE_MEMBER(ObAdminLoadBaselineArg, tenant_ids_, sql_id_, plan_hash_value_, fixed_, enabled_);

OB_SERIALIZE_MEMBER(ObLoadBaselineArg, is_all_tenant_, tenant_id_, sql_id_, plan_hash_value_, fixed_, enabled_);

OB_SERIALIZE_MEMBER(ObGetAllSchemaArg, schema_version_, tenant_name_);

bool ObAdminMergeArg::is_valid() const
{
  // empty zone means all zone
  return type_ >= START_MERGE && type_ <= RESUME_MERGE;
}

OB_SERIALIZE_MEMBER(ObAdminMergeArg, type_, zone_);

OB_SERIALIZE_MEMBER(ObAdminClearRoottableArg, tenant_name_);

OB_SERIALIZE_MEMBER(
    ObAdminSetConfigItem, name_, value_, comment_, zone_, server_, tenant_name_, exec_tenant_id_, tenant_ids_);

OB_SERIALIZE_MEMBER(ObAdminSetConfigArg, items_, is_inner_);

OB_SERIALIZE_MEMBER(ObAdminServerArg, servers_, zone_, force_stop_, op_);

OB_SERIALIZE_MEMBER(
    ObAdminZoneArg, zone_, region_, idc_, alter_zone_options_, zone_type_, sql_stmt_str_, force_stop_, op_);

OB_SERIALIZE_MEMBER(ObAutoincSyncArg, tenant_id_, table_id_, column_id_, table_part_num_, auto_increment_, sync_value_);

OB_SERIALIZE_MEMBER(ObDropReplicaArg, partition_key_, member_);
OB_SERIALIZE_MEMBER(ObAdminChangeReplicaArg, partition_key_, member_, force_cmd_);

bool ObAdminChangeReplicaArg::is_valid() const
{
  return partition_key_.is_valid();
}

bool ObUpdateIndexStatusArg::is_allow_when_disable_ddl() const
{
  bool bret = false;
  if (is_error_index_status(status_, false /* not dropped*/)) {
    bret = true;
  }
  return bret;
}

bool ObUpdateIndexStatusArg::is_valid() const
{
  return OB_INVALID_ID != index_table_id_ && status_ > INDEX_STATUS_NOT_FOUND && status_ < INDEX_STATUS_MAX;
}

OB_SERIALIZE_MEMBER((ObUpdateIndexStatusArg, ObDDLArg), index_table_id_, status_, create_mem_version_, convert_status_);

OB_SERIALIZE_MEMBER(ObMergeFinishArg, server_, frozen_version_);

bool ObMergeErrorArg::is_valid() const
{
  return partition_key_.is_valid() && server_.is_valid() && error_code_ < OB_MAX_ERROR_CODE;
}

OB_SERIALIZE_MEMBER(ObMergeErrorArg, partition_key_, server_, error_code_);

OB_SERIALIZE_MEMBER(ObAdminRebuildReplicaArg, key_, server_);

OB_SERIALIZE_MEMBER(ObDebugSyncActionArg, reset_, clear_, action_);

OB_SERIALIZE_MEMBER(ObAdminMigrateUnitArg, unit_id_, is_cancel_, destination_);

OB_SERIALIZE_MEMBER(
    ObRootMajorFreezeArg, try_frozen_version_, launch_new_round_, ignore_server_list_, svr_, tenant_id_, force_launch_);

OB_SERIALIZE_MEMBER(ObMinorFreezeArg, tenant_ids_, partition_key_);

OB_SERIALIZE_MEMBER(ObRootMinorFreezeArg, tenant_ids_, partition_key_, server_list_, zone_);

OB_SERIALIZE_MEMBER(ObSyncPartitionTableFinishArg, server_, version_);
OB_SERIALIZE_MEMBER(ObSyncPGPartitionMTFinishArg, server_, version_);

OB_SERIALIZE_MEMBER(ObCheckDanglingReplicaFinishArg, server_, version_, dangling_count_);

OB_SERIALIZE_MEMBER(
    ObMemberListAndLeaderArg, member_list_, leader_, self_, lower_list_, replica_type_, property_, role_);

bool ObMemberListAndLeaderArg::is_valid() const
{
  return member_list_.count() > 0 && self_.is_valid() && common::REPLICA_TYPE_MAX != replica_type_ &&
         property_.is_valid() && (common::INVALID_ROLE <= role_ && role_ <= common::STANDBY_LEADER);
}

// If it is a leader, you need to ensure the consistency of role_, leader_/restore_leader_, and self_
bool ObMemberListAndLeaderArg::check_leader_is_valid() const
{
  bool bret = true;
  if (is_leader_by_election(role_)) {
    bret = (leader_.is_valid() && self_ == leader_);
  }
  return bret;
}

void ObMemberListAndLeaderArg::reset()
{
  member_list_.reset();
  leader_.reset();
  self_.reset();
  lower_list_.reset();
  replica_type_ = common::REPLICA_TYPE_MAX;
  role_ = common::INVALID_ROLE;
}

int ObMemberListAndLeaderArg::assign(const ObMemberListAndLeaderArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(member_list_.assign(other.member_list_))) {
    LOG_WARN("fail to assign member_list", KR(ret), K_(member_list));
  } else if (OB_FAIL(lower_list_.assign(other.lower_list_))) {
    LOG_WARN("fail to assign lower_list", KR(ret), K_(lower_list));
  } else {
    leader_ = other.leader_;
    self_ = other.self_;
    replica_type_ = other.replica_type_;
    property_ = other.property_;
    role_ = other.role_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObGetMemberListAndLeaderResult, member_list_, leader_, self_, lower_list_, replica_type_, property_);

void ObGetMemberListAndLeaderResult::reset()
{
  member_list_.reset();
  leader_.reset();
  self_.reset();
  lower_list_.reset();
  replica_type_ = common::REPLICA_TYPE_MAX;
}

int ObGetMemberListAndLeaderResult::assign(const ObGetMemberListAndLeaderResult& other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (OB_FAIL(member_list_.assign(other.member_list_))) {
    LOG_WARN("failed to assign member list", K(ret));
  } else if (OB_FAIL(lower_list_.assign(other.lower_list_))) {
    LOG_WARN("fail to assign member list", K(ret));
  } else {
    leader_ = other.leader_;
    self_ = other.self_;
    replica_type_ = other.replica_type_;
    property_ = other.property_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBatchGetRoleArg, keys_);

void ObBatchGetRoleArg::reset()
{
  keys_.reset();
}

bool ObBatchGetRoleArg::is_valid() const
{
  return keys_.count() > 0;
}

OB_SERIALIZE_MEMBER(ObBatchGetRoleResult, results_);

void ObBatchGetRoleResult::reset()
{
  results_.reset();
}

bool ObBatchGetRoleResult::is_valid() const
{
  return results_.count() > 0;
}

bool ObCreateOutlineArg::is_valid() const
{
  return OB_INVALID_ID != outline_info_.get_tenant_id() && !outline_info_.get_name_str().empty() &&
         !(outline_info_.get_signature_str().empty() &&
             !ObOutlineInfo::is_sql_id_valid(outline_info_.get_sql_id_str())) &&
         (!outline_info_.get_outline_content_str().empty() || outline_info_.has_outline_params()) &&
         !(outline_info_.get_sql_text_str().empty() && !ObOutlineInfo::is_sql_id_valid(outline_info_.get_sql_id_str()));
}

OB_SERIALIZE_MEMBER((ObCreateOutlineArg, ObDDLArg), or_replace_, outline_info_, db_name_);

OB_SERIALIZE_MEMBER((ObCreateUserDefinedFunctionArg, ObDDLArg), udf_);

OB_SERIALIZE_MEMBER((ObDropUserDefinedFunctionArg, ObDDLArg), tenant_id_, name_, if_exist_);

OB_SERIALIZE_MEMBER((ObAlterOutlineArg, ObDDLArg), alter_outline_info_, db_name_);

bool ObDropOutlineArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !db_name_.empty() && !outline_name_.empty();
}

OB_SERIALIZE_MEMBER((ObDropOutlineArg, ObDDLArg), tenant_id_, db_name_, outline_name_);

bool ObCreateDbLinkArg::is_valid() const
{
  return OB_INVALID_ID != dblink_info_.get_tenant_id() && OB_INVALID_ID != dblink_info_.get_owner_id() &&
         OB_INVALID_ID != dblink_info_.get_dblink_id() &&
         !dblink_info_.get_dblink_name().empty()
         //      && !dblink_info_.get_cluster_name().empty()
         && !dblink_info_.get_tenant_name().empty() && !dblink_info_.get_user_name().empty() &&
         !dblink_info_.get_password().empty() && dblink_info_.get_host_ip() > 0 && dblink_info_.get_host_port() > 0;
}

OB_SERIALIZE_MEMBER((ObCreateDbLinkArg, ObDDLArg), dblink_info_);

bool ObDropDbLinkArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !dblink_name_.empty();
}

OB_SERIALIZE_MEMBER((ObDropDbLinkArg, ObDDLArg), tenant_id_, dblink_name_);

OB_SERIALIZE_MEMBER(ObSwitchLeaderListArg, partition_key_list_, leader_addr_);
OB_SERIALIZE_MEMBER(ObGetPartitionCountResult, partition_count_);

OB_SERIALIZE_MEMBER(ObFetchAliveServerArg, cluster_id_);

OB_SERIALIZE_MEMBER(ObFetchAliveServerResult, active_server_list_, inactive_server_list_);

OB_SERIALIZE_MEMBER(ObAdminSetTPArg, event_no_, event_name_, occur_, trigger_freq_, error_code_, server_, zone_);

OB_SERIALIZE_MEMBER(ObCancelTaskArg, task_id_);
OB_SERIALIZE_MEMBER(ObReportSingleReplicaArg, partition_key_);
OB_SERIALIZE_MEMBER(ObSetDiskValidArg);
OB_SERIALIZE_MEMBER((ObCreateSynonymArg, ObDDLArg), or_replace_, synonym_info_, db_name_, obj_db_name_);

OB_SERIALIZE_MEMBER((ObDropSynonymArg, ObDDLArg), tenant_id_, is_force_, db_name_, synonym_name_);

OB_SERIALIZE_MEMBER((ObCreatePlanBaselineArg, ObDDLArg), plan_baseline_info_, is_replace_);

OB_SERIALIZE_MEMBER((ObAlterPlanBaselineArg, ObDDLArg), plan_baseline_info_, field_update_bitmap_);

OB_SERIALIZE_MEMBER((ObDropPlanBaselineArg, ObDDLArg), tenant_id_, sql_id_, plan_hash_value_);

bool ObDropSynonymArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !db_name_.empty() && !synonym_name_.empty();
}

bool ObDropPlanBaselineArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER(ObAdminClearBalanceTaskArg, tenant_ids_, type_, zone_names_);

OB_SERIALIZE_MEMBER(ObMCLogInfo, log_id_, timestamp_);
OB_SERIALIZE_MEMBER(ObChangeMemberArg, partition_key_, member_, quorum_);
OB_SERIALIZE_MEMBER(ObChangeMemberCtx, partition_key_, ret_value_, log_info_);
OB_SERIALIZE_MEMBER(ObChangeMemberCtxsWrapper, result_code_, ctxs_);
OB_SERIALIZE_MEMBER(ObMemberMajorSSTableCheckArg, pkey_, table_ids_);

DEF_TO_STRING(ObForceSwitchILogFileArg)
{
  int64_t pos = 0;
  J_KV(K(force_));
  return pos;
}

OB_SERIALIZE_MEMBER(ObForceSwitchILogFileArg, force_);

DEF_TO_STRING(ObForceSetAllAsSingleReplicaArg)
{
  int64_t pos = 0;
  J_KV(K(force_));
  return pos;
}

OB_SERIALIZE_MEMBER(ObForceSetAllAsSingleReplicaArg, force_);

DEF_TO_STRING(ObForceSetServerListArg)
{
  int64_t pos = 0;
  J_KV(K(server_list_), K(replica_num_));
  return pos;
}

OB_SERIALIZE_MEMBER(ObForceSetServerListArg, server_list_, replica_num_);

bool ObForceCreateSysTableArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != last_replay_log_id_ &&
         share::is_tenant_table(table_id_);
}

DEF_TO_STRING(ObForceCreateSysTableArg)
{
  int64_t pos = 0;
  J_KV(K(tenant_id_), K(table_id_), K(last_replay_log_id_));
  return pos;
}

OB_SERIALIZE_MEMBER(ObForceCreateSysTableArg, tenant_id_, table_id_, last_replay_log_id_);

bool ObForceSetLocalityArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != exec_tenant_id_ && locality_.length() > 0;
}
int ObForceSetLocalityArg::assign(const ObForceSetLocalityArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    locality_ = other.locality_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER((ObForceSetLocalityArg, ObDDLArg), locality_);

DEF_TO_STRING(ObRootSplitPartitionArg)
{
  int64_t pos = 0;
  J_KV(K(table_id_));
  return pos;
}

int ObRootSplitPartitionArg::assign(const ObRootSplitPartitionArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    table_id_ = other.table_id_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER((ObRootSplitPartitionArg, ObDDLArg), table_id_);

OB_SERIALIZE_MEMBER(ObSplitPartitionArg, split_info_);
OB_SERIALIZE_MEMBER(ObSplitPartitionResult, results_);
OB_SERIALIZE_MEMBER(ObQueryMaxDecidedTransVersionRequest, partition_array_, last_max_decided_trans_version_);
OB_SERIALIZE_MEMBER(ObQueryMaxDecidedTransVersionResponse, ret_value_, trans_version_, pkey_);
OB_SERIALIZE_MEMBER(ObQueryIsValidMemberRequest, self_addr_, partition_array_);
OB_SERIALIZE_MEMBER(ObQueryIsValidMemberResponse, ret_value_, partition_array_, candidates_status_, ret_array_);
OB_SERIALIZE_MEMBER(ObQueryMaxFlushedILogIdRequest, partition_array_);
OB_SERIALIZE_MEMBER(ObQueryMaxFlushedILogIdResponse, err_code_, partition_array_, max_flushed_ilog_ids_);

DEF_TO_STRING(ObUpdateStatCacheArg)
{
  int64_t pos = 0;
  J_KV(K_(table_id), K_(tenant_id), K_(partition_ids), K_(column_ids));
  return pos;
}
OB_SERIALIZE_MEMBER(ObUpdateStatCacheArg, tenant_id_, table_id_, partition_ids_, column_ids_);
OB_SERIALIZE_MEMBER(ObSplitPartitionBatchArg, split_info_);
OB_SERIALIZE_MEMBER(ObSplitPartitionBatchRes, ret_list_);
OB_SERIALIZE_MEMBER((ObSequenceDDLArg, ObDDLArg), stmt_type_, option_bitset_, seq_schema_, database_name_);

OB_SERIALIZE_MEMBER(ObBootstrapArg, server_list_, cluster_type_, initial_frozen_version_, initial_schema_version_,
    primary_cluster_id_, primary_rs_list_, freeze_schemas_, frozen_status_);

int ObBootstrapArg::assign(const ObBootstrapArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_list_.assign(arg.server_list_))) {
    LOG_WARN("fail to assign", KR(ret), K(arg));
  } else if (OB_FAIL(primary_rs_list_.assign(arg.primary_rs_list_))) {
    LOG_WARN("fail to assign", KR(ret), K(arg));
  } else if (OB_FAIL(freeze_schemas_.assign(arg.freeze_schemas_))) {
    LOG_WARN("failed to assign freeze schemas", KR(ret), K(arg));
  } else if (OB_FAIL(frozen_status_.assign(arg.frozen_status_))) {
    LOG_WARN("failed to assign frozen status", KR(ret), K(arg));
  } else {
    cluster_type_ = arg.cluster_type_;
    primary_cluster_id_ = arg.primary_cluster_id_;
    initial_frozen_version_ = arg.initial_frozen_version_;
    initial_schema_version_ = arg.initial_schema_version_;
  }
  return ret;
}

bool ObSplitPartitionBatchArg::is_valid() const
{
  return split_info_.is_valid();
}

int ObSplitPartitionBatchArg::assign(const ObSplitPartitionBatchArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(split_info_, other.split_info_))) {
    SHARE_LOG(WARN, "failed to assign args_", K(ret));
  }
  return ret;
}

int ObSplitPartitionBatchRes::assign(const ObSplitPartitionBatchRes& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(ret_list_, other.ret_list_))) {
    SHARE_LOG(WARN, "failed to assign ret_list_", K(ret));
  }
  return ret;
}

DEF_TO_STRING(ObSplitPartitionBatchRes)
{
  int64_t pos = 0;
  J_KV(K_(ret_list));
  return pos;
}

DEF_TO_STRING(ObSplitPartitionBatchArg)
{
  int64_t pos = 0;
  J_KV(K_(split_info));
  return pos;
}

bool ObBatchStartElectionArg::is_valid() const
{
  return pkeys_.count() > 0 && member_list_.is_valid() && switch_type_ != INVALID_TYPE;
}

int ObBatchStartElectionArg::add_partition_key(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (pkeys_.count() >= MAX_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("fail to add partition key", K(ret), "count", pkeys_.count());
  } else if (OB_FAIL(pkeys_.push_back(pkey))) {
    LOG_WARN("fail to push back", K(ret), K(pkey));
  }
  return ret;
}

void ObBatchStartElectionArg::reset()
{
  member_list_.reset();
  pkeys_.reset();
  lease_start_time_ = 0;
  switch_timestamp_ = 0;
  switch_type_ = INVALID_TYPE;
  trace_id_.reset();
}

int ObBatchStartElectionArg::assign(const ObBatchStartElectionArg& other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(member_list_.deep_copy(other.member_list_))) {
    LOG_WARN("fail to assign server list", KR(ret));
  } else if (OB_FAIL(pkeys_.assign(other.pkeys_))) {
    LOG_WARN("fail to assign", KR(ret));
  } else {
    lease_start_time_ = other.lease_start_time_;
    leader_ = other.leader_;
    switch_timestamp_ = other.switch_timestamp_;
    switch_type_ = other.switch_type_;
    quorum_ = other.quorum_;
    trace_id_ = other.trace_id_;
  }
  return ret;
}

int ObAlterClusterInfoArg::assign(ObAlterClusterInfoArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(arg))) {
    LOG_WARN("fail to assign ddl arg", KR(ret), K(arg));
  } else {
    op_type_ = arg.op_type_;
    mode_ = arg.mode_;
    level_ = arg.level_;
    is_force_ = arg.is_force_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObAlterClusterInfoArg, ObDDLArg), op_type_, mode_, is_force_, level_);
OB_SERIALIZE_MEMBER(ObAdminClusterArg, cluster_name_, cluster_id_, alter_type_, is_force_, ddl_stmt_str_,
    rootservice_list_, redo_transport_options_);
OB_SERIALIZE_MEMBER(ObGetWRSArg, tenant_id_, scope_, need_filter_);
OB_SERIALIZE_MEMBER(ObGetWRSResult, self_addr_, err_code_, replica_wrs_info_list_);
OB_SERIALIZE_MEMBER(ObBatchStartElectionArg, lease_start_time_, pkeys_, member_list_, leader_, switch_timestamp_,
    switch_type_, quorum_, trace_id_);
OB_SERIALIZE_MEMBER(ObClusterActionVerifyArg, verify_type_);
bool ObGetWRSArg::is_valid() const
{
  return (OB_INVALID_ID != tenant_id_ && INVALID_RANGE != scope_);
}

int64_t ObEstPartArgElement::get_serialize_size(void) const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(scan_flag_);
  OB_UNIS_ADD_LEN(index_id_);
  OB_UNIS_ADD_LEN(range_columns_count_);
  OB_UNIS_ADD_LEN(batch_);

  return len;
}

int ObEstPartArgElement::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(scan_flag_);
  OB_UNIS_ENCODE(index_id_);
  OB_UNIS_ENCODE(range_columns_count_);
  OB_UNIS_ENCODE(batch_);

  return ret;
}

int ObEstPartArgElement::deserialize(
    common::ObIAllocator& allocator, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(scan_flag_);
  OB_UNIS_DECODE(index_id_);
  OB_UNIS_DECODE(range_columns_count_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(batch_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("fail to deserialize batch", K(ret), K(data_len), K(pos));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObEstPartArg)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(pkey_);
  OB_UNIS_ADD_LEN(schema_version_);
  OB_UNIS_ADD_LEN(column_ids_);
  OB_UNIS_ADD_LEN(partition_keys_);
  OB_UNIS_ADD_LEN(scan_param_);
  OB_UNIS_ADD_LEN(index_params_);
  OB_UNIS_ADD_LEN(index_pkeys_);
  return len;
}

OB_DEF_SERIALIZE(ObEstPartArg)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(pkey_);
  OB_UNIS_ENCODE(schema_version_);
  OB_UNIS_ENCODE(column_ids_);
  OB_UNIS_ENCODE(partition_keys_);
  OB_UNIS_ENCODE(scan_param_);
  OB_UNIS_ENCODE(index_params_);
  OB_UNIS_ENCODE(index_pkeys_);
  return ret;
}

OB_DEF_DESERIALIZE(ObEstPartArg)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(pkey_);
  OB_UNIS_DECODE(schema_version_);
  OB_UNIS_DECODE(column_ids_);
  OB_UNIS_DECODE(partition_keys_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scan_param_.deserialize(allocator_, buf, data_len, pos))) {
      SQL_OPT_LOG(WARN, "fail to deserialize scan param", K(ret));
    }
  }
  int64_t N = 0;
  OB_UNIS_DECODE(N);
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    ObEstPartArgElement arg;
    if (OB_FAIL(arg.deserialize(allocator_, buf, data_len, pos))) {
      SQL_OPT_LOG(WARN, "fail to deserialize index param", K(ret));
    } else if (OB_FAIL(index_params_.push_back(arg))) {
      SQL_OPT_LOG(WARN, "failed to push back arg element", K(ret));
    }
  }
  OB_UNIS_DECODE(index_pkeys_);
  return ret;
}

OB_SERIALIZE_MEMBER(ObEstPartResElement, logical_row_count_, physical_row_count_, reliable_, est_records_);

OB_SERIALIZE_MEMBER(ObEstPartRes, part_rowcount_size_res_, index_param_res_);

OB_SERIALIZE_MEMBER(ObEstPartRowCountSizeRes, row_count_, part_size_, avg_row_size_, reliable_);
OB_SERIALIZE_MEMBER(
    TenantServerUnitConfig, tenant_id_, compat_mode_, unit_config_, replica_type_, is_delete_, if_not_grant_);

OB_SERIALIZE_MEMBER((ObDDLNopOpreatorArg, ObDDLArg), schema_operation_);
OB_SERIALIZE_MEMBER(ObTenantSchemaVersions, tenant_schema_versions_);

OB_SERIALIZE_MEMBER(
    TenantIdAndStats, tenant_id_, refreshed_schema_version_, ddl_lag_, min_sys_table_scn_, min_user_table_scn_);

OB_SERIALIZE_MEMBER(ObClusterTenantStats, tenant_stats_array_);

bool TenantServerUnitConfig::is_valid() const
{
  return common::OB_INVALID_ID != tenant_id_ &&
         ((share::ObWorker::CompatMode::INVALID != compat_mode_ && unit_config_.is_valid() &&
              replica_type_ != common::ObReplicaType::REPLICA_TYPE_MAX) ||
             (is_delete_));
}

int TenantServerUnitConfig::init(const uint64_t tenant_id, const share::ObWorker::CompatMode compat_mode,
    const share::ObUnitConfig& unit_config, const common::ObReplicaType replica_type, const bool if_not_grant,
    const bool is_delete)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
    // do not check others validation, since their validations vary
  } else {
    tenant_id_ = tenant_id;
    compat_mode_ = compat_mode;
    unit_config_ = unit_config;
    replica_type_ = replica_type;
    if_not_grant_ = if_not_grant;
    is_delete_ = is_delete;
  }
  return ret;
}

int ObTenantSchemaVersions::add(const int64_t tenant_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(schema_version));
  } else {
    TenantIdAndSchemaVersion info;
    info.tenant_id_ = tenant_id;
    info.schema_version_ = schema_version;
    if (OB_FAIL(tenant_schema_versions_.push_back(info))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}
int ObStandbyHeartBeatRes::set_primary_addr(const share::ObClusterAddr& primary_addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!primary_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("primary cluster addr is invalid", KR(ret), K(primary_addr));
  } else if (OB_FAIL(primary_addr_.assign(primary_addr))) {
    LOG_WARN("failed to assign primary cluster addr", KR(ret), K(primary_addr));
  }
  return ret;
}
int ObStandbyHeartBeatRes::set_standby_addr(const share::ObClusterAddr& standby_addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!standby_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("standby cluster addr is invalid", KR(ret), K(standby_addr));
  } else if (OB_FAIL(standby_addr_.assign(standby_addr))) {
    LOG_WARN("failed to assign standby cluster addr", KR(ret), K(standby_addr));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObGetSchemaArg, ObDDLArg), reserve_, ignore_fail_);
bool ObFinishSchemaSplitArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != tenant_id_ &&
         (rootserver::ObRsJobType::JOB_TYPE_SCHEMA_SPLIT == type_ ||
             rootserver::ObRsJobType::JOB_TYPE_SCHEMA_SPLIT_V2 == type_);
}
int ObFinishSchemaSplitArg::assign(const ObFinishSchemaSplitArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    type_ = other.type_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER((ObFinishSchemaSplitArg, ObDDLArg), tenant_id_, type_);
OB_SERIALIZE_MEMBER(ObBroadcastSchemaArg, tenant_id_, schema_version_);

void ObBroadcastSchemaArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  schema_version_ = OB_INVALID_VERSION;
}

OB_SERIALIZE_MEMBER(ObCheckMergeFinishArg, frozen_version_);
bool ObCheckMergeFinishArg::is_valid() const
{
  return frozen_version_ > 0;
}

OB_SERIALIZE_MEMBER(ObGetRecycleSchemaVersionsArg, tenant_ids_);
bool ObGetRecycleSchemaVersionsArg::is_valid() const
{
  return tenant_ids_.count() > 0;
}
void ObGetRecycleSchemaVersionsArg::reset()
{
  tenant_ids_.reset();
}
int ObGetRecycleSchemaVersionsArg::assign(const ObGetRecycleSchemaVersionsArg& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(tenant_ids_.assign(other.tenant_ids_))) {
      LOG_WARN("fail to assign tenant_ids", KR(ret), K(other));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObGetRecycleSchemaVersionsResult, recycle_schema_versions_);
bool ObGetRecycleSchemaVersionsResult::is_valid() const
{
  return recycle_schema_versions_.count() > 0;
}
void ObGetRecycleSchemaVersionsResult::reset()
{
  recycle_schema_versions_.reset();
}
int ObGetRecycleSchemaVersionsResult::assign(const ObGetRecycleSchemaVersionsResult& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(recycle_schema_versions_.assign(other.recycle_schema_versions_))) {
      LOG_WARN("fail to assign recycle_schema_versions", KR(ret), K(other));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObGetClusterInfoArg, need_check_sync_, max_primary_schema_version_, primary_schema_versions_,
    cluster_version_, standby_became_primary_scn_);
bool ObGetClusterInfoArg::is_valid() const
{
  bool bret = true;
  if (need_check_sync_ && OB_INVALID_VERSION == max_primary_schema_version_ &&
      0 < primary_schema_versions_.tenant_schema_versions_.count() && OB_INVALID_ID != cluster_version_) {
    bret = false;
  }
  return bret;
}
void ObGetClusterInfoArg::reset()
{
  need_check_sync_ = false;
  max_primary_schema_version_ = OB_INVALID_VERSION;
  primary_schema_versions_.reset();
  cluster_version_ = OB_INVALID_ID;
  standby_became_primary_scn_ = OB_INVALID_VERSION;
}
OB_SERIALIZE_MEMBER(ObCheckAddStandbyArg, cluster_version_);
OB_SERIALIZE_MEMBER(ObHaGtsPingRequest, gts_id_, req_id_, epoch_id_, request_ts_);
OB_SERIALIZE_MEMBER(ObHaGtsPingResponse, gts_id_, req_id_, epoch_id_, response_ts_);
OB_SERIALIZE_MEMBER(ObHaGtsGetRequest, gts_id_, self_addr_, tenant_id_, srr_.mts_);
OB_SERIALIZE_MEMBER(ObHaGtsGetResponse, gts_id_, tenant_id_, srr_.mts_, gts_);
OB_SERIALIZE_MEMBER(ObHaGtsHeartbeat, gts_id_, addr_);
OB_SERIALIZE_MEMBER(ObHaGtsUpdateMetaRequest, gts_id_, epoch_id_, member_list_, local_ts_);
OB_SERIALIZE_MEMBER(ObHaGtsUpdateMetaResponse, local_ts_);
OB_SERIALIZE_MEMBER(ObHaGtsChangeMemberRequest, gts_id_, offline_replica_);
OB_SERIALIZE_MEMBER(ObHaGtsChangeMemberResponse, ret_value_);

OB_SERIALIZE_MEMBER(ObAdminAddDiskArg, diskgroup_name_, disk_path_, alias_name_, server_, zone_);
OB_SERIALIZE_MEMBER(ObAdminDropDiskArg, diskgroup_name_, alias_name_, server_, zone_);

OB_SERIALIZE_MEMBER(ObSchemaSnapshotRes, schema_version_, frozen_version_, table_schemas_, tenant_schemas_, user_infos_,
    db_privs_, table_privs_, freeze_schemas_, tenant_flashback_scn_, failover_timestamp_, frozen_status_,
    sys_schema_changed_, cluster_name_);

OB_SERIALIZE_MEMBER(ObSchemaSnapshotArg, schema_version_, frozen_version_);

bool ObSchemaSnapshotRes::is_valid() const
{
  return schema_version_ >= 0 && frozen_version_ >= 0;
}

ObSchemaSnapshotRes::ObSchemaSnapshotRes()
    : schema_version_(0),
      frozen_version_(0),
      table_schemas_(),
      tenant_schemas_(),
      user_infos_(),
      db_privs_(),
      table_privs_(),
      freeze_schemas_(),
      failover_timestamp_(OB_INVALID_VERSION),
      tenant_flashback_scn_(),
      frozen_status_(),
      sys_schema_changed_(false),
      cluster_name_()
{}

int ObSchemaSnapshotRes::assign(const ObSchemaSnapshotRes& res)
{
  int ret = OB_SUCCESS;
  if (!res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("res is invalid", KR(ret), K(res));
  } else if (OB_FAIL(table_schemas_.assign(res.table_schemas_))) {
    LOG_WARN("failed to assign table schema", KR(ret), K(res));
  } else if (OB_FAIL(tenant_schemas_.assign(res.tenant_schemas_))) {
    LOG_WARN("failed to assign tenant schemas", KR(ret), K(res));
  } else if (OB_FAIL(user_infos_.assign(res.user_infos_))) {
    LOG_WARN("failed to assign user info", KR(ret), K(res));
  } else if (OB_FAIL(db_privs_.assign(res.db_privs_))) {
    LOG_WARN("failed to assign db privs", KR(ret), K(res));
  } else if (OB_FAIL(table_privs_.assign(res.table_privs_))) {
    LOG_WARN("failed to assign table privs", KR(ret), K(res));
  } else if (OB_FAIL(freeze_schemas_.assign(res.freeze_schemas_))) {
    LOG_WARN("failed to assign freeze schemas", KR(ret), K(res));
  } else if (OB_FAIL(tenant_flashback_scn_.assign(res.tenant_flashback_scn_))) {
    LOG_WARN("failed to assign tenant flashback scn", KR(ret), K(res));
  } else if (OB_FAIL(frozen_status_.assign(res.frozen_status_))) {
    LOG_WARN("failed to assign frozen status", KR(ret), K(res));
  } else if (OB_FAIL(cluster_name_.assign(res.cluster_name_))) {
    LOG_WARN("failed to assign clulster name", KR(ret), K(res));
  } else {
    schema_version_ = res.schema_version_;
    frozen_version_ = res.frozen_version_;
    failover_timestamp_ = res.failover_timestamp_;
    sys_schema_changed_ = res.sys_schema_changed_;
  }
  return ret;
}
void ObSchemaSnapshotRes::reset()
{
  schema_version_ = 0;
  frozen_version_ = 0;
  table_schemas_.reset();
  tenant_schemas_.reset();
  user_infos_.reset();
  db_privs_.reset();
  table_privs_.reset();
  freeze_schemas_.reset();
  tenant_flashback_scn_.reset();
  failover_timestamp_ = OB_INVALID_VERSION;
  frozen_status_.reset();
  sys_schema_changed_ = false;
  cluster_name_.reset();
}

void ObAlterTableResArg::reset()
{
  schema_type_ = OB_MAX_SCHEMA;
  schema_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
}

void ObAlterTableRes::reset()
{
  index_table_id_ = OB_INVALID_ID;
  constriant_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  res_arg_array_.reset();
}

void ObRegistClusterRes::reset()
{
  cluster_idx_ = OB_INVALID_INDEX;
  login_name_.reset();
  login_passwd_.reset();
  primary_cluster_.reset();
}

int ObRegistClusterRes::set_primary_cluster(const share::ObClusterAddr& cluster_addr)
{
  int ret = OB_SUCCESS;
  primary_cluster_.reset();
  if (!cluster_addr.is_valid() || OB_INVALID_INDEX == cluster_addr.cluster_idx_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster_addr is invalid", K(ret), K(cluster_addr));
  } else if (OB_FAIL(primary_cluster_.assign(cluster_addr))) {
    LOG_WARN("failed to assign cluster addr", K(ret), K(cluster_addr));
  }
  return ret;
}

bool ObClusterInfoArg::is_valid() const
{
  return cluster_info_.is_valid();
}

OB_SERIALIZE_MEMBER(ObAlterTableResArg, schema_type_, schema_id_, schema_version_);
OB_SERIALIZE_MEMBER(ObAlterTableRes, index_table_id_, constriant_id_, schema_version_, res_arg_array_);
int ObClusterInfoArg::assign(const ObClusterInfoArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cluster_info_.assign(other.cluster_info_))) {
    LOG_WARN("failed to assign cluster info", KR(ret), K(other));
  } else if (OB_FAIL(sync_cluster_ids_.assign(other.sync_cluster_ids_))) {
    LOG_WARN("failed to assign cluster ids", KR(ret), K(other), "this", *this);
  } else if (OB_FAIL(redo_options_.assign(other.redo_options_))) {
    LOG_WARN("failed to assign redo options", KR(ret), K(other));
  } else {
    server_status_ = other.server_status_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObClusterInfoArg, cluster_info_, server_status_, sync_cluster_ids_, redo_options_);
OB_SERIALIZE_MEMBER(ObRegistClusterArg, cluster_name_, cluster_id_, pre_regist_, cluster_addr_);
OB_SERIALIZE_MEMBER(ObRegistClusterRes, cluster_idx_, login_name_, login_passwd_, primary_cluster_);

OB_SERIALIZE_MEMBER(ObBatchFlashbackArg, switchover_timestamp_, flashback_to_ts_, leader_, pkeys_, flashback_from_ts_,
    frozen_timestamp_, is_logical_flashback_, query_end_time_, schema_version_);
void ObBatchFlashbackArg::reset()
{
  switchover_timestamp_ = 0;
  flashback_to_ts_ = 0;
  leader_.reset();
  pkeys_.reset();
  flashback_from_ts_ = 0;
  frozen_timestamp_ = 0;
  is_logical_flashback_ = false;
  query_end_time_ = 0;
  schema_version_ = OB_INVALID_VERSION;
}
int ObBatchFlashbackArg::add_pkey(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_FAIL(pkeys_.push_back(pkey))) {
    SHARE_LOG(WARN, "fail to push back", KR(ret), K(pkey));
  }
  return ret;
}

bool ObBatchFlashbackArg::is_valid() const
{
  return ((is_logical_flashback_ && flashback_from_ts_ == 0)         // failover use
             || (!is_logical_flashback_ && flashback_from_ts_ > 0))  // flashback table use
         && flashback_to_ts_ > 0 && leader_.is_valid() && pkeys_.count() > 0 && frozen_timestamp_ >= 0 &&
         query_end_time_ > 0;
}

OB_SERIALIZE_MEMBER(
    ObStandbyHeartBeatRes, primary_addr_, standby_addr_, primary_schema_info_, protection_mode_, tenant_schema_vers_);
OB_SERIALIZE_MEMBER(ObGetSwitchoverStatusRes, switchover_status_, switchover_info_);
OB_SERIALIZE_MEMBER(ObGetTenantSchemaVersionArg, tenant_id_);
OB_SERIALIZE_MEMBER(ObGetTenantSchemaVersionResult, schema_version_);
OB_SERIALIZE_MEMBER((ObFinishReplayArg, ObDDLArg), schema_version_);
OB_SERIALIZE_MEMBER(ObTenantMemoryArg, tenant_id_, memory_size_, refresh_interval_);
OB_SERIALIZE_MEMBER(
    ObCheckStandbyCanAccessArg, failover_epoch_, last_merged_version_, cluster_status_, tenant_flashback_scn_);

OB_SERIALIZE_MEMBER(ObCheckServerEmptyArg, mode_);
OB_SERIALIZE_MEMBER(ObCheckDeploymentModeArg, single_zone_deployment_on_);

bool ObForceDropSchemaArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != exec_tenant_id_ && OB_INVALID_ID != exec_tenant_id_ && recycle_schema_version_ >= 0 &&
         OB_INVALID_ID != schema_id_ && share::schema::OB_MAX_SCHEMA != type_;
}

int ObForceDropSchemaArg::assign(const ObForceDropSchemaArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(partition_ids_.assign(other.partition_ids_))) {
    LOG_WARN("fail to assign partition_ids", K(ret));
  } else if (OB_FAIL(subpartition_ids_.assign(other.subpartition_ids_))) {
    LOG_WARN("fail to assign subpartition_ids", K(ret));
  } else {
    recycle_schema_version_ = other.recycle_schema_version_;
    schema_id_ = other.schema_id_;
    type_ = other.type_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    (ObForceDropSchemaArg, ObDDLArg), recycle_schema_version_, schema_id_, partition_ids_, type_, subpartition_ids_);
OB_SERIALIZE_MEMBER(ObArchiveLogArg, enable_);

OB_SERIALIZE_MEMBER(ObBackupDatabaseArg, tenant_id_, is_incremental_, encryption_mode_, passwd_);
ObBackupDatabaseArg::ObBackupDatabaseArg()
    : tenant_id_(0), is_incremental_(true), encryption_mode_(share::ObBackupEncryptionMode::NONE), passwd_()
{}

bool ObBackupDatabaseArg::is_valid() const
{
  return share::ObBackupEncryptionMode::is_valid(encryption_mode_);
}

OB_SERIALIZE_MEMBER(ObPGBackupArchiveLogArg, archive_round_, pg_key_);

ObPGBackupArchiveLogArg::ObPGBackupArchiveLogArg() : archive_round_(-1), pg_key_()
{}

bool ObPGBackupArchiveLogArg::is_valid() const
{
  return archive_round_ > 0 && pg_key_.is_valid();
}

int ObPGBackupArchiveLogArg::assign(const ObPGBackupArchiveLogArg& arg)
{
  int ret = OB_SUCCESS;
  archive_round_ = arg.archive_round_;
  pg_key_ = arg.pg_key_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObPGBackupArchiveLogRes, result_, finished_, pg_key_, checkpoint_ts_);

ObPGBackupArchiveLogRes::ObPGBackupArchiveLogRes() : result_(0), finished_(false), checkpoint_ts_(), pg_key_()
{}

bool ObPGBackupArchiveLogRes::is_valid() const
{
  return pg_key_.is_valid();
}

int ObPGBackupArchiveLogRes::assign(const ObPGBackupArchiveLogRes& arg)
{
  int ret = OB_SUCCESS;
  result_ = arg.result_;
  finished_ = arg.finished_;
  pg_key_ = arg.pg_key_;
  checkpoint_ts_ = arg.checkpoint_ts_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupArchiveLogBatchArg, tenant_id_, archive_round_, piece_id_, create_date_, job_id_,
    checkpoint_ts_, task_id_, src_root_path_, src_storage_info_, dst_root_path_, dst_storage_info_, arg_array_);

ObBackupArchiveLogBatchArg::ObBackupArchiveLogBatchArg()
    : tenant_id_(OB_INVALID_ID),
      archive_round_(-1),
      piece_id_(-1),
      create_date_(-1),
      job_id_(-1),
      checkpoint_ts_(-1),
      task_id_(),
      src_root_path_(),
      src_storage_info_(),
      dst_root_path_(),
      dst_storage_info_(),
      arg_array_()
{}

bool ObBackupArchiveLogBatchArg::is_valid() const
{
  bool bret = true;
  for (int64_t i = 0; bret && i < arg_array_.count(); ++i) {
    const ObPGBackupArchiveLogArg& arg = arg_array_.at(i);
    if (!arg.is_valid()) {
      bret = false;
    }
  }
  return bret;
}

int ObBackupArchiveLogBatchArg::assign(const ObBackupArchiveLogBatchArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(arg_array_, arg.arg_array_))) {
    SHARE_LOG(WARN, "failed to assign arg_array_", K(ret));
  } else {
    tenant_id_ = arg.tenant_id_;
    archive_round_ = arg.archive_round_;
    piece_id_ = arg.piece_id_;
    create_date_ = arg.create_date_;
    job_id_ = arg.job_id_;
    checkpoint_ts_ = arg.checkpoint_ts_;
    task_id_ = arg.task_id_;
    STRNCPY(src_root_path_, arg.src_root_path_, OB_MAX_BACKUP_PATH_LENGTH);
    STRNCPY(src_storage_info_, arg.src_storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
    STRNCPY(dst_root_path_, arg.dst_root_path_, OB_MAX_BACKUP_PATH_LENGTH);
    STRNCPY(dst_storage_info_, arg.dst_storage_info_, OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObBackupArchiveLogBatchRes, server_, tenant_id_, archive_round_, piece_id_, job_id_, checkpoint_ts_, res_array_);

ObBackupArchiveLogBatchRes::ObBackupArchiveLogBatchRes()
    : server_(), tenant_id_(OB_INVALID_ID), archive_round_(0), piece_id_(0), job_id_(0), checkpoint_ts_(0), res_array_()
{}

bool ObBackupArchiveLogBatchRes::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && archive_round_ > 0 && piece_id_ >= 0 && server_.is_valid();
}

bool ObBackupArchiveLogBatchRes::is_interrupted() const
{
  bool bret = false;
  for (int64_t i = 0; i < res_array_.count(); ++i) {
    const ObPGBackupArchiveLogRes& res = res_array_.at(i);
    if (OB_LOG_ARCHIVE_INTERRUPTED == res.result_) {
      bret = true;
      break;
    }
  }
  return bret;
}

int ObBackupArchiveLogBatchRes::assign(const ObBackupArchiveLogBatchRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(res_array_, arg.res_array_))) {
    SHARE_LOG(WARN, "failed to assgin res array", KR(ret));
  } else {
    tenant_id_ = arg.tenant_id_;
    archive_round_ = arg.archive_round_;
    piece_id_ = arg.piece_id_;
    job_id_ = arg.job_id_;
    checkpoint_ts_ = arg.checkpoint_ts_;
    server_ = arg.server_;
  }
  return ret;
}

int ObBackupArchiveLogBatchRes::get_min_checkpoint_ts(int64_t& checkpoint_ts) const
{
  int ret = OB_SUCCESS;
  checkpoint_ts = INT64_MAX;
  for (int64_t i = 0; OB_SUCC(ret) && i < res_array_.count(); ++i) {
    const ObPGBackupArchiveLogRes& res = res_array_.at(i);
    checkpoint_ts = std::min(checkpoint_ts, res.checkpoint_ts_);
  }
  return ret;
}

int ObBackupArchiveLogBatchRes::get_finished_pg_list(common::ObIArray<common::ObPGKey>& pg_list) const
{
  int ret = OB_SUCCESS;
  pg_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < res_array_.count(); ++i) {
    const ObPGBackupArchiveLogRes& res = res_array_.at(i);
    const bool finished = OB_SUCCESS == res.result_;
    if (finished) {
      if (OB_FAIL(pg_list.push_back(res.pg_key_))) {
        SHARE_LOG(WARN, "failed to push back pg key", KR(ret), K(res));
      }
    }
  }
  return ret;
}

int ObBackupArchiveLogBatchRes::get_failed_pg_list(common::ObIArray<common::ObPGKey>& pg_list) const
{
  int ret = OB_SUCCESS;
  pg_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < res_array_.count(); ++i) {
    const ObPGBackupArchiveLogRes& res = res_array_.at(i);
    if (OB_SUCCESS != res.result_) {
      if (OB_FAIL(pg_list.push_back(res.pg_key_))) {
        SHARE_LOG(WARN, "failed to push back pg key", KR(ret), K(res));
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMigrateBackupsetArg, backup_set_id_, pg_key_, backup_backupset_arg_);
ObMigrateBackupsetArg::ObMigrateBackupsetArg() : backup_set_id_(-1), pg_key_(), backup_backupset_arg_()
{}

int ObMigrateBackupsetArg::assign(const ObMigrateBackupsetArg& arg)
{
  int ret = OB_SUCCESS;
  backup_set_id_ = arg.backup_set_id_;
  pg_key_ = arg.pg_key_;
  backup_backupset_arg_ = arg.backup_backupset_arg_;
  return ret;
}

bool ObMigrateBackupsetArg::is_valid() const
{
  return backup_set_id_ > 0 && pg_key_.is_valid() && backup_backupset_arg_.is_valid();
}

OB_SERIALIZE_MEMBER(
    ObBackupBackupsetArg, tenant_id_, backup_set_id_, tenant_name_, backup_backup_dest_, max_backup_times_);

ObBackupBackupsetArg::ObBackupBackupsetArg()
    : tenant_id_(OB_INVALID_ID), backup_set_id_(-1), tenant_name_(), backup_backup_dest_(""), max_backup_times_(-1)
{}

bool ObBackupBackupsetArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && backup_set_id_ >= 0;
}

int ObBackupBackupsetArg::assign(const ObBackupBackupsetArg& o)
{
  int ret = OB_SUCCESS;

  tenant_id_ = o.tenant_id_;
  backup_set_id_ = o.backup_set_id_;
  tenant_name_ = o.tenant_name_;
  MEMCPY(backup_backup_dest_, o.backup_backup_dest_, sizeof(backup_backup_dest_));
  max_backup_times_ = o.max_backup_times_;
  return ret;
}

int ObBackupArchiveLogArg::assign(const ObBackupArchiveLogArg& o)
{
  int ret = OB_SUCCESS;

  enable_ = o.enable_;
  return ret;
}

int ObBackupBackupPieceArg::assign(const ObBackupBackupPieceArg& o)
{
  int ret = OB_SUCCESS;

  tenant_id_ = o.tenant_id_;
  piece_id_ = o.piece_id_;
  tenant_name_ = o.tenant_name_;
  MEMCPY(backup_backup_dest_, o.backup_backup_dest_, sizeof(backup_backup_dest_));
  max_backup_times_ = o.max_backup_times_;
  backup_all_ = o.backup_all_;
  with_active_piece_ = o.with_active_piece_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupBackupsetBatchArg, arg_array_, timeout_ts_, task_id_, tenant_dropped_);

int ObBackupBackupsetBatchArg::assign(const ObBackupBackupsetBatchArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(arg_array_, arg.arg_array_))) {
    SHARE_LOG(WARN, "failed to assign res_array", KR(ret));
  } else {
    timeout_ts_ = arg.timeout_ts_;
    task_id_ = arg.task_id_;
    tenant_dropped_ = arg.tenant_dropped_;
  }
  return ret;
}

bool ObBackupBackupsetBatchArg::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < arg_array_.count() && is_valid; ++i) {
    is_valid = arg_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObBackupBackupsetReplicaRes, key_, dst_, arg_, result_);
ObBackupBackupsetReplicaRes::ObBackupBackupsetReplicaRes()
{}

int ObBackupBackupsetReplicaRes::assign(const ObBackupBackupsetReplicaRes& res)
{
  int ret = OB_SUCCESS;
  key_ = res.key_;
  dst_ = res.dst_;
  arg_ = res.arg_;
  result_ = res.result_;
  return ret;
}

bool ObBackupBackupsetReplicaRes::is_valid() const
{
  return key_.is_valid() && dst_.is_valid() && arg_.is_valid();
}

OB_SERIALIZE_MEMBER(ObBackupBackupsetBatchRes, res_array_);

int ObBackupBackupsetBatchRes::assign(const ObBackupBackupsetBatchRes& res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(res_array_, res.res_array_))) {
    SHARE_LOG(WARN, "failed to assign res_array", KR(ret));
  }
  return ret;
}

bool ObBackupBackupsetBatchRes::is_valid() const
{
  bool is_valid = true;
  for (int64_t i = 0; i < res_array_.count() && is_valid; ++i) {
    is_valid = res_array_.at(i).is_valid();
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObBackupArchiveLogArg, enable_);
OB_SERIALIZE_MEMBER(ObBackupBackupPieceArg, tenant_id_, piece_id_, tenant_name_, backup_backup_dest_, max_backup_times_,
    backup_all_, with_active_piece_);
OB_SERIALIZE_MEMBER(ObBackupManageArg, tenant_id_, type_, value_, copy_id_);
OB_SERIALIZE_MEMBER(
    CheckLeaderRpcIndex, switchover_timestamp_, epoch_, ml_pk_index_, pkey_info_start_index_, tenant_id_);
OB_SERIALIZE_MEMBER(ObBatchCheckLeaderArg, pkeys_, index_, trace_id_);
OB_SERIALIZE_MEMBER(ObBatchCheckRes, results_, index_);
OB_SERIALIZE_MEMBER(ObCheckFlashbackInfoArg, min_weak_read_timestamp_);
OB_SERIALIZE_MEMBER(ObCheckFlashbackInfoResult, addr_, pkey_, result_, switchover_timestamp_, ret_code_);
OB_SERIALIZE_MEMBER(ObBatchWriteCutdataClogArg, pkeys_, index_, trace_id_, switchover_timestamp_, flashback_ts_,
    schema_version_, query_end_time_);

ObBackupBackupPieceArg::ObBackupBackupPieceArg()
    : tenant_id_(OB_INVALID_ID),
      piece_id_(-1),
      tenant_name_(),
      backup_backup_dest_(""),
      max_backup_times_(-1),
      backup_all_(false)
{}

int ObCheckFlashbackInfoArg::assign(const ObCheckFlashbackInfoArg& other)
{
  int ret = OB_SUCCESS;
  min_weak_read_timestamp_ = other.min_weak_read_timestamp_;
  return ret;
}

int ObCheckFlashbackInfoResult::assign(const ObCheckFlashbackInfoResult& other)
{
  int ret = OB_SUCCESS;
  pkey_ = other.pkey_;
  addr_ = other.addr_;
  result_ = other.result_;
  switchover_timestamp_ = other.switchover_timestamp_;
  ret_code_ = other.ret_code_;
  return ret;
}

bool CheckLeaderRpcIndex::is_valid() const
{
  return switchover_timestamp_ > 0 && epoch_ >= 0 && ml_pk_index_ >= 0 && pkey_info_start_index_ >= 0 && tenant_id_ > 0;
}

void CheckLeaderRpcIndex::reset()
{
  switchover_timestamp_ = 0;
  epoch_ = -1;
  ml_pk_index_ = -1;
  pkey_info_start_index_ = -1;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

bool ObBatchWriteCutdataClogArg::is_valid() const
{
  return 0 < pkeys_.count() && index_.is_valid() && 0 < switchover_timestamp_ && 0 < flashback_ts_ &&
         0 < schema_version_;
}

void ObBatchWriteCutdataClogArg::reset()
{
  pkeys_.reset();
  index_.reset();
  switchover_timestamp_ = 0;
  flashback_ts_ = 0;
  schema_version_ = 0;
  trace_id_.reset();
  query_end_time_ = 0;
}

bool ObBatchCheckLeaderArg::is_valid() const
{
  return pkeys_.count() > 0 && index_.is_valid();
}

void ObBatchCheckLeaderArg::reset()
{
  pkeys_.reset();
  index_.reset();
}

bool ObBatchCheckRes::is_valid() const
{
  return results_.count() > 0 && index_.is_valid();
}

void ObBatchCheckRes::reset()
{
  results_.reset();
  index_.reset();
}
OB_SERIALIZE_MEMBER(ObRebuildIndexInRestoreArg, tenant_id_);
OB_SERIALIZE_MEMBER((ObUpdateTableSchemaVersionArg, ObDDLArg), tenant_id_, table_id_, schema_version_, action_);

bool ObUpdateTableSchemaVersionArg::is_allow_when_upgrade() const
{
  return UPDATE_SYS_TABLE_IN_TENANT_SPACE != action_;
}

bool ObUpdateTableSchemaVersionArg::is_valid() const
{
  return (tenant_id_ > OB_INVALID_TENANT_ID && table_id_ >= 0 && schema_version_ >= OB_INVALID_SCHEMA_VERSION) ||
         UPDATE_SYS_TABLE_IN_TENANT_SPACE == action_;
}

void ObUpdateTableSchemaVersionArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_SCHEMA_VERSION;
  action_ = Action::INVALID;
}

void ObUpdateTableSchemaVersionArg::init(const int64_t tenant_id, const int64_t table_id, const int64_t schema_version,
    const bool is_replay_schema, const Action action)
{
  exec_tenant_id_ = OB_SYS_TENANT_ID;
  tenant_id_ = tenant_id;
  table_id_ = table_id;
  schema_version_ = schema_version;
  is_replay_schema_ = is_replay_schema;
  action_ = action;
}

int ObUpdateTableSchemaVersionArg::assign(const ObUpdateTableSchemaVersionArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ObDDLArg", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    table_id_ = other.table_id_;
    schema_version_ = other.schema_version_;
    action_ = other.action_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObRestoreModifySchemaArg, ObDDLArg), type_, schema_id_);
bool ObRestoreModifySchemaArg::is_valid() const
{
  return INVALID_TYPE < type_ && MAX_TYPE > type_ && schema_id_ > 0;
}

OB_SERIALIZE_MEMBER(ObPreProcessServerArg, server_, rescue_server_);
int ObPreProcessServerArg::init(const common::ObAddr& server, const common::ObAddr& rescue_server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid() || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(rescue_server));
  } else {
    server_ = server;
    rescue_server_ = rescue_server;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPreProcessServerReplyArg, server_, rescue_server_, ret_code_);
int ObPreProcessServerReplyArg::init(
    const common::ObAddr& server, const common::ObAddr& rescue_server, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid() || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(rescue_server));
  } else {
    server_ = server;
    rescue_server_ = rescue_server;
    ret_code_ = ret_code;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObAdminRollingUpgradeArg, stage_);

bool ObAdminRollingUpgradeArg::is_valid() const
{
  return OB_UPGRADE_STAGE_DBUPGRADE <= stage_ && stage_ <= OB_UPGRADE_STAGE_POSTUPGRADE;
}

OB_SERIALIZE_MEMBER(ObRsListArg, rs_list_, master_rs_);
ObLocationRpcRenewArg::ObLocationRpcRenewArg(common::ObIAllocator& allocator)
    : keys_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator))
{}
OB_SERIALIZE_MEMBER(ObLocationRpcRenewArg, keys_);
OB_SERIALIZE_MEMBER(ObLocationRpcRenewResult, results_);
OB_SERIALIZE_MEMBER(ObGetMasterKeyResultArg, str_);
OB_SERIALIZE_MEMBER(ObKillPartTransCtxArg, partition_key_, trans_id_);
OB_SERIALIZE_MEMBER(ObPhysicalRestoreResult, job_id_, return_ret_, mod_, tenant_id_, trace_id_, addr_);

ObPhysicalRestoreResult::ObPhysicalRestoreResult()
    : job_id_(common::OB_INVALID_ID),
      return_ret_(common::OB_SUCCESS),
      mod_(share::PHYSICAL_RESTORE_MOD_MAX_NUM),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      trace_id_(),
      addr_()
{}

bool ObPhysicalRestoreResult::is_valid() const
{
  return job_id_ > 0 && OB_SUCCESS != return_ret_ && mod_ >= share::PHYSICAL_RESTORE_MOD_RS &&
         mod_ < share::PHYSICAL_RESTORE_MOD_MAX_NUM && addr_.is_valid();
}

int ObPhysicalRestoreResult::assign(const ObPhysicalRestoreResult& other)
{
  int ret = OB_SUCCESS;

  job_id_ = other.job_id_;
  return_ret_ = other.return_ret_;
  mod_ = other.mod_;
  tenant_id_ = other.tenant_id_;
  trace_id_ = other.trace_id_;
  addr_ = other.addr_;
  return ret;
}

bool ObExecuteRangePartSplitArg::is_valid() const
{
  return partition_key_.is_valid() && rowkey_.is_valid();
}

int ObExecuteRangePartSplitArg::assign(const ObExecuteRangePartSplitArg& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    partition_key_ = other.partition_key_;
    rowkey_ = other.rowkey_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObExecuteRangePartSplitArg, ObDDLArg), partition_key_, rowkey_);
OB_SERIALIZE_MEMBER(ObRefreshTimezoneArg, tenant_id_);
OB_SERIALIZE_MEMBER(ObCreateRestorePointArg, tenant_id_, name_);
OB_SERIALIZE_MEMBER(ObDropRestorePointArg, tenant_id_, name_);

OB_SERIALIZE_MEMBER(ObCheckBuildIndexTaskExistArg, tenant_id_, task_id_, scheduler_id_);

OB_SERIALIZE_MEMBER(ObPartitionBroadcastArg, keys_);
bool ObPartitionBroadcastArg::is_valid() const
{
  return keys_.count() > 0;
}
int ObPartitionBroadcastArg::assign(const ObPartitionBroadcastArg& other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(keys_.assign(other.keys_))) {
    LOG_WARN("fail to assign keys", KR(ret), K(other));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPartitionBroadcastResult, ret_);
bool ObPartitionBroadcastResult::is_valid() const
{
  return true;
}
int ObPartitionBroadcastResult::assign(const ObPartitionBroadcastResult& other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ret_ = other.ret_;
  }
  return ret;
}

int ObSubmitBuildIndexTaskArg::assign(const ObSubmitBuildIndexTaskArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    index_tid_ = other.index_tid_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObSubmitBuildIndexTaskArg, ObDDLArg), index_tid_);

OB_SERIALIZE_MEMBER(ObFetchSstableSizeArg, pkey_, index_id_);

int ObFetchSstableSizeArg::assign(const ObFetchSstableSizeArg &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    pkey_ = other.pkey_;
    index_id_ = other.index_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObFetchSstableSizeRes, size_);

int ObFetchSstableSizeRes::assign(const ObFetchSstableSizeRes &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    size_ = other.size_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObTableTTLArg, cmd_code_);
ObTableTTLArg::ObTableTTLArg()
    : cmd_code_(4)
{}
int ObTableTTLArg::assign(const ObTableTTLArg& other) {
  cmd_code_ = other.cmd_code_;
  return OB_SUCCESS;
}


OB_SERIALIZE_MEMBER(ObTTLResponseArg, tenant_id_, task_id_, server_addr_, task_status_);
ObTTLResponseArg::ObTTLResponseArg()
    : tenant_id_(0),
      task_id_(OB_INVALID_ID),
      server_addr_(),
      task_status_(15)
{}

OB_SERIALIZE_MEMBER(ObTTLRequestArg, cmd_code_, trigger_type_, task_id_, tenant_id_);
OB_SERIALIZE_MEMBER(ObTTLResult, ret_code_);

int ObTTLRequestArg::assign(const ObTTLRequestArg &other) {
  int ret = OB_SUCCESS;

  cmd_code_ = other.cmd_code_;
  task_id_ = other.task_id_;
  tenant_id_ = other.tenant_id_;
  trigger_type_ = other.trigger_type_;
  
  return ret;
}

}  // end namespace obrpc
}  // namespace oceanbase
