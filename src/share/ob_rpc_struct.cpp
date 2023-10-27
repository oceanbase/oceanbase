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
#include "lib/utility/ob_serialization_helper.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_store_format.h"
#include "observer/ob_server_struct.h"
#include "storage/tx/ob_trans_service.h"
#include "share/ls/ob_ls_status_operator.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share::schema;
using namespace share;
using namespace storage;
using namespace transaction;
namespace obrpc
{
OB_SERIALIZE_MEMBER(Bool, v_);
OB_SERIALIZE_MEMBER(Int64, v_);
OB_SERIALIZE_MEMBER(UInt64, v_);

static const char* upgrade_stage_str[OB_UPGRADE_STAGE_MAX] = {
  "NULL",
  "NONE",
  "PREUPGRADE",
  "DBUPGRADE",
  "POSTUPGRADE"
};

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

ObUpgradeStage get_upgrade_stage(const ObString &str)
{
  ObUpgradeStage stage = OB_UPGRADE_STAGE_INVALID;
  for(int64_t i = OB_UPGRADE_STAGE_NONE; i < OB_UPGRADE_STAGE_MAX; i++) {
    if (0 == str.case_compare(upgrade_stage_str[i])) {
      stage = static_cast<ObUpgradeStage>(i);
      break;
    }
  }
  return stage;
}

int ObDDLArg::assign(const ObDDLArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(based_schema_object_infos_.assign(other.based_schema_object_infos_))) {
    LOG_WARN("fail to assign based_schema_object_infos", KR(ret));
  } else {
    ddl_stmt_str_ = other.ddl_stmt_str_;
    exec_tenant_id_ = other.exec_tenant_id_;
    ddl_id_str_ = other.ddl_id_str_;
    sync_from_primary_ = other.sync_from_primary_;
    parallelism_ = other.parallelism_;
    task_id_ = other.task_id_;
    consumer_group_id_ = other.consumer_group_id_;
  }
  return ret;
}

DEF_TO_STRING(ObDDLArg)
{
  int64_t pos = 0;
  J_KV("ddl_stmt_str", contain_sensitive_data() ? ObString(OB_MASKED_STR) : ddl_stmt_str_,
       K_(exec_tenant_id),
       K_(ddl_id_str),
       K_(sync_from_primary),
       K_(based_schema_object_infos),
       K_(parallelism),
       K_(task_id),
       K_(consumer_group_id));
  return pos;
}

DEF_TO_STRING(ObGetRootserverRoleResult)
{
  int64_t pos = 0;
  J_KV(K_(role),
       K_(status));
  return pos;
}

int ObGetRootserverRoleResult::init(
    const common::ObRole &role,
    const share::status::ObRootServiceStatus &status)
{
  int ret = OB_SUCCESS;
  role_ = role;
  status_ = status;
  return ret;
}

void ObGetRootserverRoleResult::reset()
{
  role_ = FOLLOWER;
  status_ = status::MAX;
}

int ObGetRootserverRoleResult::assign(const ObGetRootserverRoleResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    role_ = other.role_;     //int32_t
    status_ = other.status_; //ObRootServiceStatus is enum
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObGetRootserverRoleResult, role_, status_);

DEF_TO_STRING(ObServerInfo)
{
  int64_t pos = 0;
  J_KV("region", region_,
       "zone", zone_,
       "server", server_);
  return pos;
}

OB_SERIALIZE_MEMBER(ObServerInfo,
                    zone_,
                    server_,
                    region_);

DEF_TO_STRING(ObPartitionId)
{
  int64_t pos = 0;
  J_KV(KT_(table_id),
       K_(partition_id));
  return pos;
}

OB_SERIALIZE_MEMBER(ObPartitionId,
                    table_id_,
                    partition_id_);

//////////////////////////////////////////////
// ObClonePartitionArg
//DEF_TO_STRING(ObClonePartitionArg)
//{
//  int64_t pos = 0;
//  J_KV(K_(partition_key),
//       K_(migrate_version),
//       K_(last_sstable_index),
//       K_(last_block_index));
//  return pos;
//}
//
//OB_SERIALIZE_MEMBER(ObClonePartitionArg,
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
int ObCreateResourceUnitArg::init(const common::ObString &name,
    const ObUnitResource &ur,
    const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAlterResourceUnitArg::init(name, ur))) {
    LOG_WARN("init resource unit arg fail", KR(ret), K(name), K(ur));
  } else {
    if_not_exist_ = if_not_exist;
  }
  return ret;
}

int ObCreateResourceUnitArg::assign(const ObCreateResourceUnitArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAlterResourceUnitArg::assign(other))) {
    LOG_WARN("fail to assign resource unit arg", KR(ret), K(other));
  } else {
    if_not_exist_ = other.if_not_exist_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateResourceUnitArg, ObAlterResourceUnitArg),
                    if_not_exist_);

OB_SERIALIZE_MEMBER((ObSplitResourcePoolArg, ObDDLArg),
                    pool_name_,
                    zone_list_,
                    split_pool_list_);

bool ObSplitResourcePoolArg::is_valid() const
{
  return !pool_name_.empty() && zone_list_.count() > 0 && split_pool_list_.count() > 0;
}

int ObSplitResourcePoolArg::assign(const ObSplitResourcePoolArg &other)
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

OB_SERIALIZE_MEMBER((ObAlterResourceTenantArg, ObDDLArg),
                    tenant_name_,
                    unit_num_,
                    unit_group_id_array_);

bool ObAlterResourceTenantArg::is_valid() const
{
  // invalid unit_group_id_ means picking unit_group_id_ automatically
  return !tenant_name_.empty() && unit_num_ > 0;
}

int ObAlterResourceTenantArg::assign(
    const ObAlterResourceTenantArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    tenant_name_ = other.tenant_name_;
    unit_num_ = other.unit_num_;
    if (OB_FAIL(unit_group_id_array_.assign(other.unit_group_id_array_))) {
      LOG_WARN("fail to assign array", KR(ret));
    }
  }
  return ret;
}

int ObAlterResourceTenantArg::fill_unit_group_id(
    const uint64_t unit_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(unit_group_id_array_.push_back(unit_group_id))) {
    LOG_WARN("faill to push back", KR(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObMergeResourcePoolArg, ObDDLArg),
                    old_pool_list_,
                    new_pool_list_);

bool ObMergeResourcePoolArg::is_valid() const
{
  return old_pool_list_.count() > 0 && new_pool_list_.count() > 0 && new_pool_list_.count() < 2;
}

int ObMergeResourcePoolArg::assign(const ObMergeResourcePoolArg &other)
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

bool ObStartRedefTableArg::is_valid() const
{
  return (OB_INVALID_ID != orig_tenant_id_ && OB_INVALID_ID != orig_table_id_
          && OB_INVALID_ID != target_tenant_id_ && OB_INVALID_ID != target_table_id_
          && share::DDL_INVALID != ddl_type_);
}
int ObStartRedefTableArg::set_nls_formats(const common::ObString *nls_formats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(nls_formats)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("nls_formats is nullptr", K(ret));
  } else {
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; ++i) {
      if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(nls_formats[i].length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory!", K(ret), "size: ", nls_formats[i].length());
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

int ObStartRedefTableArg::assign(const ObStartRedefTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_.assign(arg.tz_info_))) {
    LOG_WARN("tz_info assign failed", K(ret));
  } else if (OB_FAIL(tz_info_wrap_.deep_copy(arg.tz_info_wrap_))) {
    LOG_WARN("failed to deep_copy tz info wrap", K(ret), "tz_info_wrap", arg.tz_info_wrap_);
  } else if (FALSE_IT(ddl_stmt_str_.assign_ptr(arg.ddl_stmt_str_.ptr(), static_cast<int32_t>(arg.ddl_stmt_str_.length())))) {
    // do nothing
  } else {
    orig_tenant_id_ = arg.orig_tenant_id_;
    orig_table_id_ = arg.orig_table_id_;
    target_tenant_id_ = arg.target_tenant_id_;
    target_table_id_ = arg.target_table_id_;
    session_id_ = arg.session_id_;
    parallelism_ = arg.parallelism_;
    ddl_type_ = arg.ddl_type_;
    trace_id_ = arg.trace_id_;
    sql_mode_ = arg.sql_mode_;
    for (int64_t i = 0; OB_SUCC(ret) && i < common::ObNLSFormatEnum::NLS_MAX; i++) {
      nls_formats_[i].assign_ptr(arg.nls_formats_[i].ptr(), static_cast<int32_t>(arg.nls_formats_[i].length()));
    }
  }
  return ret;
}

int ObStartRedefTableRes::assign(const ObStartRedefTableRes &res)
{
  int ret = OB_SUCCESS;
  task_id_ = res.task_id_;
  tenant_id_ = res.tenant_id_;
  schema_version_ = res.schema_version_;
  return ret;
}

OB_DEF_SERIALIZE(ObStartRedefTableArg)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
          orig_tenant_id_,
          orig_table_id_,
          target_tenant_id_,
          target_table_id_,
          session_id_,
          parallelism_,
          ddl_type_,
          ddl_stmt_str_,
          trace_id_,
          sql_mode_,
          tz_info_,
          tz_info_wrap_);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
        if (OB_FAIL(nls_formats_[i].serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize nls_formats_[i]", K(ret), K(nls_formats_[i]));
        }
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObStartRedefTableArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
          orig_tenant_id_,
          orig_table_id_,
          target_tenant_id_,
          target_table_id_,
          session_id_,
          parallelism_,
          ddl_type_,
          ddl_stmt_str_,
          trace_id_,
          sql_mode_,
          tz_info_,
          tz_info_wrap_);
  if (OB_SUCC(ret)) {
    ObString tmp_string;
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
      if (OB_FAIL(tmp_string.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize nls_formats_", K(ret), K(i));
      } else if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(tmp_string.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory!", K(ret));
      } else {
        MEMCPY(tmp_ptr[i], tmp_string.ptr(), tmp_string.length());
        nls_formats_[i].assign_ptr(tmp_ptr[i], tmp_string.length());
        tmp_string.reset();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; i++) {
        allocator_.free(tmp_ptr[i]);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObStartRedefTableArg)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
          orig_tenant_id_,
          orig_table_id_,
          target_tenant_id_,
          target_table_id_,
          session_id_,
          parallelism_,
          ddl_type_,
          ddl_stmt_str_,
          trace_id_,
          sql_mode_,
          tz_info_,
          tz_info_wrap_);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; i++) {
        len += nls_formats_[i].get_serialize_size();
      }
    }
  }
  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

bool ObCopyTableDependentsArg::is_valid() const
{
  return 0 != task_id_ && OB_INVALID_ID != tenant_id_;
}

int ObCopyTableDependentsArg::assign(const ObCopyTableDependentsArg &arg)
{
  int ret = OB_SUCCESS;
  task_id_ = arg.task_id_;
  tenant_id_ = arg.tenant_id_;
  copy_indexes_ = arg.copy_indexes_;
  copy_triggers_ = arg.copy_triggers_;
  copy_constraints_ = arg.copy_constraints_;
  copy_foreign_keys_ = arg.copy_foreign_keys_;
  ignore_errors_ = arg.ignore_errors_;
  return ret;
}

bool ObFinishRedefTableArg::is_valid() const
{
  return (0 != task_id_ && OB_INVALID_ID != tenant_id_);
}

int ObFinishRedefTableArg::assign(const ObFinishRedefTableArg &arg)
{
  int ret = OB_SUCCESS;
  task_id_ = arg.task_id_;
  tenant_id_ = arg.tenant_id_;
  return ret;
}

bool ObAbortRedefTableArg::is_valid() const
{
  return 0 != task_id_ && OB_INVALID_ID != tenant_id_;
}

int ObAbortRedefTableArg::assign(const ObAbortRedefTableArg &arg)
{
  int ret = OB_SUCCESS;
  task_id_ = arg.task_id_;
  tenant_id_ = arg.tenant_id_;
  return ret;
}

bool ObUpdateDDLTaskActiveTimeArg::is_valid() const
{
  return 0 != task_id_ && OB_INVALID_ID != tenant_id_;
}

int ObUpdateDDLTaskActiveTimeArg::assign(const ObUpdateDDLTaskActiveTimeArg &arg)
{
  int ret = OB_SUCCESS;
  task_id_ = arg.task_id_;
  tenant_id_ = arg.tenant_id_;
  return ret;
}

ObRecoverRestoreTableDDLArg::~ObRecoverRestoreTableDDLArg()
{
  reset();
}

void ObRecoverRestoreTableDDLArg::reset()
{
  target_schema_.reset();
  src_tenant_id_ = common::OB_INVALID_ID;
  src_table_id_ = common::OB_INVALID_ID;
  ddl_task_id_ = common::OB_INVALID_ID;
  allocator_.reset();
  ObDDLArg::reset();
}

bool ObRecoverRestoreTableDDLArg::is_valid() const
{
  return OB_INVALID_ID != target_schema_.get_tenant_id()
         && OB_INVALID_ID != target_schema_.get_database_id()
         && !target_schema_.get_table_name_str().empty()
         && OB_INVALID_ID != src_tenant_id_
         && OB_INVALID_ID != src_table_id_
         && ddl_task_id_ > 0;
}

int ObRecoverRestoreTableDDLArg::assign(const ObRecoverRestoreTableDDLArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(other));
  } else if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("assign failed", K(ret));
  } else if (OB_FAIL(target_schema_.assign(other.target_schema_))) {
    LOG_WARN("assign failed", K(ret));
  } else if (OB_FAIL(tz_info_.assign(other.tz_info_))) {
    LOG_WARN("assign failed", K(ret));
  } else if (OB_FAIL(tz_info_wrap_.deep_copy(other.tz_info_wrap_))) {
    LOG_WARN("assign failed", K(ret));
  } else {
    src_tenant_id_ = other.src_tenant_id_;
    src_table_id_ = other.src_table_id_;
    ddl_task_id_ = ddl_task_id_;
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; ++i) {
      const ObString &cur_str = other.nls_formats_[i];
      if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(cur_str.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), "size", cur_str.length());
      } else {
        MEMCPY(tmp_ptr[i], cur_str.ptr(), cur_str.length());
        nls_formats_[i].assign_ptr(tmp_ptr[i], cur_str.length());
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

OB_DEF_SERIALIZE(ObRecoverRestoreTableDDLArg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else if (OB_FAIL(ObDDLArg::serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize DDLArg", K(ret), K(buf_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
                target_schema_,
                src_tenant_id_,
                src_table_id_,
                ddl_task_id_,
                tz_info_,
                tz_info_wrap_);
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
      if (OB_FAIL(nls_formats_[i].serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize nls_formats_[i]", K(ret), K(nls_formats_[i]));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObRecoverRestoreTableDDLArg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize DDLArg", K(ret), K(data_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
                target_schema_,
                src_tenant_id_,
                src_table_id_,
                ddl_task_id_,
                tz_info_,
                tz_info_wrap_);
    ObString tmp_string;
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
      if (OB_FAIL(tmp_string.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize nls_formats_", K(ret), K(i));
      } else if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(tmp_string.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory!", K(ret));
      } else {
        MEMCPY(tmp_ptr[i], tmp_string.ptr(), tmp_string.length());
        nls_formats_[i].assign_ptr(tmp_ptr[i], tmp_string.length());
        tmp_string.reset();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; i++) {
        allocator_.free(tmp_ptr[i]);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRecoverRestoreTableDDLArg)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    len += ObDDLArg::get_serialize_size();
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                target_schema_,
                src_tenant_id_,
                src_table_id_,
                ddl_task_id_,
                tz_info_,
                tz_info_wrap_);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; i++) {
        len += nls_formats_[i].get_serialize_size();
      }
    }
  }
  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

bool ObCreateHiddenTableArg::is_valid() const
{
  return (OB_INVALID_ID != tenant_id_
          && OB_INVALID_TENANT_ID != exec_tenant_id_
          && OB_INVALID_ID != table_id_
          && OB_INVALID_ID != dest_tenant_id_
          && share::DDL_INVALID != ddl_type_);
}
int ObCreateHiddenTableArg::assign(const ObCreateHiddenTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_.assign(arg.tz_info_))) {
    LOG_WARN("tz_info assign failed", K(ret));
  } else if (OB_FAIL(tz_info_wrap_.deep_copy(arg.tz_info_wrap_))) {
    LOG_WARN("failed to deep_copy tz info wrap", K(ret), "tz_info_wrap", arg.tz_info_wrap_);
  } else if (FALSE_IT(ddl_stmt_str_.assign_ptr(arg.ddl_stmt_str_.ptr(), static_cast<int32_t>(arg.ddl_stmt_str_.length())))) {
    // do nothing
  } else {
    tenant_id_ = arg.tenant_id_;
    table_id_ = arg.table_id_;
    consumer_group_id_ = arg.consumer_group_id_;
    dest_tenant_id_ = arg.dest_tenant_id_;
    session_id_ = arg.session_id_;
    parallelism_ = arg.parallelism_;
    ddl_type_ = arg.ddl_type_;
    sql_mode_ = arg.sql_mode_;
    for (int64_t i = 0; OB_SUCC(ret) && i < common::ObNLSFormatEnum::NLS_MAX; i++) {
      nls_formats_[i].assign_ptr(arg.nls_formats_[i].ptr(), static_cast<int32_t>(arg.nls_formats_[i].length()));
    }
  }
  return ret;
}
OB_DEF_SERIALIZE(ObCreateHiddenTableArg)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ObDDLArg::serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize DDLArg", K(ret), K(buf_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
                tenant_id_,
                table_id_,
                consumer_group_id_,
                dest_tenant_id_,
                session_id_,
                parallelism_,
                ddl_type_,
                sql_mode_,
                tz_info_,
                tz_info_wrap_);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
        if (OB_FAIL(nls_formats_[i].serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize nls_formats_[i]", K(ret), K(nls_formats_[i]));
        }
      }
    }
  }
  return ret;
}
OB_DEF_DESERIALIZE(ObCreateHiddenTableArg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize DDLArg", K(ret), K(data_len), K(pos));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              table_id_,
              consumer_group_id_,
              dest_tenant_id_,
              session_id_,
              parallelism_,
              ddl_type_,
              sql_mode_,
              tz_info_,
              tz_info_wrap_);
    ObString tmp_string;
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; i++) {
      if (OB_FAIL(tmp_string.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize nls_formats_", K(ret), K(i));
      } else if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(tmp_string.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory!", K(ret));
      } else {
        MEMCPY(tmp_ptr[i], tmp_string.ptr(), tmp_string.length());
        nls_formats_[i].assign_ptr(tmp_ptr[i], tmp_string.length());
        tmp_string.reset();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; i++) {
        allocator_.free(tmp_ptr[i]);
      }
    }
  }
  return ret;
}
OB_DEF_SERIALIZE_SIZE(ObCreateHiddenTableArg)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    len += ObDDLArg::get_serialize_size();
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                tenant_id_,
                table_id_,
                consumer_group_id_,
                dest_tenant_id_,
                session_id_,
                parallelism_,
                ddl_type_,
                sql_mode_,
                tz_info_,
                tz_info_wrap_);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; i++) {
        len += nls_formats_[i].get_serialize_size();
      }
    }
  }
  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

int ObCreateHiddenTableRes::assign(const ObCreateHiddenTableRes &res)
{
  int ret = OB_SUCCESS;
  tenant_id_ = res.tenant_id_;
  table_id_ = res.table_id_;
  dest_tenant_id_ = res.dest_tenant_id_;
  dest_table_id_ = res.dest_table_id_;
  trace_id_ = res.trace_id_;
  task_id_ = res.task_id_;
  schema_version_ = res.schema_version_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObCreateHiddenTableRes,
                    tenant_id_,
                    table_id_,
                    dest_tenant_id_,
                    dest_table_id_,
                    trace_id_,
                    task_id_,
                    schema_version_);

OB_SERIALIZE_MEMBER(ObStartRedefTableRes,
                    task_id_,
                    tenant_id_,
                    schema_version_);

OB_SERIALIZE_MEMBER(ObCopyTableDependentsArg,
                    task_id_,
                    tenant_id_,
                    copy_indexes_,
                    copy_triggers_,
                    copy_constraints_,
                    copy_foreign_keys_,
                    ignore_errors_);

OB_SERIALIZE_MEMBER(ObFinishRedefTableArg,
                    task_id_,
                    tenant_id_);

OB_SERIALIZE_MEMBER(ObAbortRedefTableArg,
                    task_id_,
                    tenant_id_);

OB_SERIALIZE_MEMBER(ObUpdateDDLTaskActiveTimeArg,
                    task_id_,
                    tenant_id_);

int ObAlterResourceUnitArg::assign(const ObAlterResourceUnitArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    unit_config_ = other.unit_config_;
  }
  return ret;
}

int ObAlterResourceUnitArg::init(const common::ObString &name, const ObUnitResource &ur)
{
  int ret = OB_SUCCESS;
  const uint64_t unit_config_id = OB_INVALID_ID;

  if (OB_FAIL(unit_config_.set(unit_config_id, name, ur))) {
    LOG_WARN("init unit config fail", KR(ret), K(unit_config_id), K(name), K(ur));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObAlterResourceUnitArg, ObDDLArg),
                    unit_config_);

bool ObDropResourceUnitArg::is_valid() const
{
  return !unit_name_.empty();
}

int ObDropResourceUnitArg::assign(const ObDropResourceUnitArg &other)
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
  J_KV(K_(unit_name),
       K_(if_exist));
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropResourceUnitArg, ObDDLArg),
                    unit_name_,
                    if_exist_);

/// pool

DEF_TO_STRING(ObCreateResourcePoolArg)
{
  int64_t pos = 0;
  J_KV(K_(pool_name),
       K_(unit),
       K_(unit_num),
       K_(zone_list),
       K_(replica_type),
       K_(if_not_exist));
  return pos;
}

bool ObCreateResourcePoolArg::is_valid() const
{
  // zone_list empty means all zone, don't need to check it
  return !pool_name_.empty() && !unit_.empty() && unit_num_ > 0;
}

int ObCreateResourcePoolArg::assign(const ObCreateResourcePoolArg &other)
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
  }
  return ret;
}

int ObCreateResourcePoolArg::init(const ObString &pool_name,
           const ObString &unit_name,
           const int64_t unit_count,
           const common::ObIArray<common::ObZone> &zone_list,
           const common::ObReplicaType replica_type,
           const uint64_t exec_tenant_id,
           const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  ObDDLArg::reset();
  if (OB_UNLIKELY(pool_name.empty() || unit_name.empty()
                  || 0 >= unit_count || 0 == zone_list.count()
                  || OB_SYS_TENANT_ID != exec_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pool_name), K(unit_name), K(unit_count),
                                K(zone_list), K(exec_tenant_id));
  } else if (OB_FAIL(zone_list_.assign(zone_list))) {
    LOG_WARN("failed to assign zone list", KR(ret), K(zone_list));
  }  else {
    pool_name_ = pool_name;
    unit_ = unit_name;
    replica_type_ = replica_type;
    exec_tenant_id_ = exec_tenant_id;
    if_not_exist_ = if_not_exist;
    unit_num_ = unit_count;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateResourcePoolArg, ObDDLArg),
                    pool_name_,
                    unit_,
                    unit_num_,
                    zone_list_,
                    if_not_exist_,
                    replica_type_);

bool ObAlterResourcePoolArg::is_valid() const
{
  // unit empty means not changed, unit_num zero means not changed,
  // zone_list empty means not changed
  return !pool_name_.empty() && unit_num_ >= 0;
}

int ObAlterResourcePoolArg::assign(const ObAlterResourcePoolArg &other)
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
  J_KV(K_(pool_name),
       K_(unit),
       K_(unit_num),
       K_(zone_list),
       K_(delete_unit_id_array));
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterResourcePoolArg, ObDDLArg),
                    pool_name_,
                    unit_,
                    unit_num_,
                    zone_list_,
                    delete_unit_id_array_);

bool ObDropResourcePoolArg::is_valid() const
{
  return !pool_name_.empty();
}

int ObDropResourcePoolArg::assign(const ObDropResourcePoolArg &other)
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
  J_KV(K_(pool_name),
       K_(if_exist));
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropResourcePoolArg, ObDDLArg),
                    pool_name_,
                    if_exist_);


OB_SERIALIZE_MEMBER(ObCmdArg,
                    sql_text_);

OB_SERIALIZE_MEMBER(ObDDLArg,
                    ddl_stmt_str_,
                    exec_tenant_id_,
                    ddl_id_str_,
                    sync_from_primary_,
                    based_schema_object_infos_,
                    parallelism_,
                    task_id_,
                    consumer_group_id_);

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
  J_KV(K_(sys_id),
       K_(value));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSysVarIdValue, sys_id_, value_);

bool ObCreateTenantArg::is_valid() const
{
  return !tenant_schema_.get_tenant_name_str().empty() && pool_list_.count() > 0
         && (!is_restore_ || (is_restore_ && palf_base_info_.is_valid()
                              && recovery_until_scn_.is_valid_and_not_min()
                              && compatible_version_ > 0))
         && (!is_creating_standby_ || (is_creating_standby_ && !log_restore_source_.empty()));
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

int ObCreateTenantArg::assign(const ObCreateTenantArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(tenant_schema_.assign(other.tenant_schema_))) {
    LOG_WARN("fail to assign tenant schema", K(ret), K(other));
  } else if (OB_FAIL(pool_list_.assign(other.pool_list_))) {
    LOG_WARN("fail to assign pool list", K(ret), K(other));
  } else if (OB_FAIL(sys_var_list_.assign(other.sys_var_list_))) {
    LOG_WARN("fail to assign sys var list", K(ret), K(other));
  } else {
    if_not_exist_ = other.if_not_exist_;
    name_case_mode_ = other.name_case_mode_;
    is_restore_ = other.is_restore_;
    palf_base_info_ = other.palf_base_info_;
    recovery_until_scn_ = other.recovery_until_scn_;
    compatible_version_ = other.compatible_version_;
    is_creating_standby_ = other.is_creating_standby_;
    log_restore_source_ = other.log_restore_source_;
    is_tmp_tenant_for_recover_ = other.is_tmp_tenant_for_recover_;
  }
  return ret;
}

void ObCreateTenantArg::reset()
{
  ObDDLArg::reset();
  tenant_schema_.reset();
  pool_list_.reset();
  if_not_exist_ = false;
  sys_var_list_.reset();
  name_case_mode_ = common::OB_NAME_CASE_INVALID;
  is_restore_ = false;
  palf_base_info_.reset();
  compatible_version_ = 0;
  is_creating_standby_ = false;
  log_restore_source_.reset();
  is_tmp_tenant_for_recover_ = false;
}

int ObCreateTenantArg::init(const share::schema::ObTenantSchema &tenant_schema,
                            const common::ObIArray<common::ObString> &pool_list,
                            const bool is_sync_from_primary,
                            const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!tenant_schema.is_valid() || 0 == pool_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schema), K(pool_list));
  } else if (OB_FAIL(tenant_schema_.assign(tenant_schema))) {
    LOG_WARN("failed to assign tenant schema", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(pool_list_.assign(pool_list))) {
    LOG_WARN("failed to assign pool list", KR(ret), K(pool_list));
  } else {
    exec_tenant_id_ = OB_SYS_TENANT_ID;
    sync_from_primary_ = is_sync_from_primary;
    if_not_exist_ = if_not_exist;
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
       K_(palf_base_info),
       K_(recovery_until_scn),
       K_(compatible_version),
       K_(is_creating_standby),
       K_(log_restore_source),
       K_(is_tmp_tenant_for_recover));
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateTenantArg, ObDDLArg),
                    tenant_schema_,
                    pool_list_,
                    if_not_exist_,
                    sys_var_list_,
                    name_case_mode_,
                    is_restore_,
                    palf_base_info_,
                    compatible_version_,
                    recovery_until_scn_,
                    is_creating_standby_,
                    log_restore_source_,
                    is_tmp_tenant_for_recover_);

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

int ObCreateTenantEndArg::assign(const ObCreateTenantEndArg &other)
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

int ObModifyTenantArg::check_normal_tenant_can_do(bool &normal_can_do) const
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
      if (alter_option_bitset_.has_member(i)
          && i != PRIMARY_ZONE
          && i != ZONE_LIST
          && i != RESOURCE_POOL_LIST) {
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

int ObModifyTenantArg::assign(const ObModifyTenantArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("failed to assign other", KR(ret), K(other));
  } else if (OB_FAIL(tenant_schema_.assign(other.tenant_schema_))) {
    LOG_WARN("failed to assign tenant schema", KR(ret), K(other));
  } else if (OB_FAIL(pool_list_.assign(other.pool_list_))) {
    LOG_WARN("failed to assign pool list", KR(ret), K(other));
  } else if (OB_FAIL(sys_var_list_.assign(other.sys_var_list_))) {
    LOG_WARN("failed to assign sys variable", KR(ret), K(other));
  } else {
    alter_option_bitset_ = other.alter_option_bitset_;
    new_tenant_name_ = other.new_tenant_name_;
  }
  return ret;
}

DEF_TO_STRING(ObModifyTenantArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_schema),
       K_(pool_list),
       K_(alter_option_bitset),
       K_(sys_var_list),
       K_(new_tenant_name));
  return pos;
}

OB_SERIALIZE_MEMBER((ObModifyTenantArg, ObDDLArg),
                    tenant_schema_,
                    alter_option_bitset_,
                    pool_list_,
                    sys_var_list_,
                    new_tenant_name_);

bool ObLockTenantArg::is_valid() const
{
  return !tenant_name_.empty();
}

DEF_TO_STRING(ObLockTenantArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_name),
       K_(is_locked));
  return pos;
}

OB_SERIALIZE_MEMBER((ObLockTenantArg, ObDDLArg), tenant_name_, is_locked_);

bool ObDropTenantArg::is_valid() const
{
  bool b_ret = drop_only_in_restore_ ? force_drop_ : true;
  return !tenant_name_.empty() && b_ret;
}

DEF_TO_STRING(ObDropTenantArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_name),
       K_(if_exist),
       K_(delay_to_drop),
       K_(force_drop),
       K_(object_name),
       K_(open_recyclebin),
       K_(tenant_id),
       K_(drop_only_in_restore));
  return pos;
}

int ObDropTenantArg::assign(const ObDropTenantArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret), K(other));
  } else {
    tenant_name_ = other.tenant_name_;
    if_exist_ = other.if_exist_;
    delay_to_drop_ = other.delay_to_drop_;
    force_drop_ = other.force_drop_;
    object_name_ = other.object_name_;
    open_recyclebin_ = other.object_name_;
    tenant_id_ = other.tenant_id_;
    drop_only_in_restore_ = other.drop_only_in_restore_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDropTenantArg, ObDDLArg),
                    tenant_name_, if_exist_, delay_to_drop_,
                    force_drop_, object_name_, open_recyclebin_,
                    tenant_id_, drop_only_in_restore_);

bool ObAddSysVarArg::is_valid() const
{
  return sysvar_.is_valid();
}

int ObAddSysVarArg::assign(const ObAddSysVarArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret), K(other));
  } else if (OB_FAIL(sysvar_.assign(other.sysvar_))) {
    LOG_WARN("fail to assign sy var", KR(ret), K(other));
  } else {
    if_not_exist_ = other.if_not_exist_;
    update_sys_var_ = other.update_sys_var_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObAddSysVarArg, ObDDLArg), sysvar_, if_not_exist_, update_sys_var_);

DEF_TO_STRING(ObAddSysVarArg)
{
  int64_t pos = 0;
  J_KV(K_(sysvar), K_(if_not_exist), K_(update_sys_var));
  return pos;
}

bool ObModifySysVarArg::is_valid() const
{
  return !sys_var_list_.empty() && OB_INVALID_ID != tenant_id_;
}

int ObModifySysVarArg::assign(const ObModifySysVarArg &other)
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
  return OB_INVALID_ID != database_schema_.get_tenant_id()
      && !database_schema_.get_database_name_str().empty();
}

DEF_TO_STRING(ObCreateDatabaseArg)
{
  int64_t pos = 0;
  J_KV(K_(database_schema),
       K_(if_not_exist));
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateDatabaseArg, ObDDLArg),
                    database_schema_,
                    if_not_exist_);

bool ObAlterDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != database_schema_.get_tenant_id()
      && !database_schema_.get_database_name_str().empty();
}

DEF_TO_STRING(ObAlterDatabaseArg)
{
  int64_t pos = 0;
  J_KV(K_(database_schema));
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterDatabaseArg, ObDDLArg),
                    database_schema_,
                    alter_option_bitset_);

bool ObDropDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !database_name_.empty()
    && lib::Worker::CompatMode::INVALID != compat_mode_;
}

DEF_TO_STRING(ObDropDatabaseArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id),
       K_(database_name),
       K_(if_exist),
       K_(to_recyclebin),
       K_(is_add_to_scheduler),
       K_(compat_mode));
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropDatabaseArg, ObDDLArg),
                    tenant_id_,
                    database_name_,
                    if_exist_,
                    to_recyclebin_,
                    is_add_to_scheduler_,
                    compat_mode_);

bool ObCreateTablegroupArg::is_valid() const
{
  return OB_INVALID_ID != tablegroup_schema_.get_tenant_id()
      && !tablegroup_schema_.get_tablegroup_name().empty();
}

DEF_TO_STRING(ObCreateTablegroupArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tablegroup_schema),
       K_(if_not_exist));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateTablegroupArg, ObDDLArg),
                    tablegroup_schema_,
                    if_not_exist_)

bool ObDropTablegroupArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !tablegroup_name_.empty();
}

DEF_TO_STRING(ObDropTablegroupArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
       K_(tablegroup_name),
       K_(if_exist));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropTablegroupArg, ObDDLArg),
                    tenant_id_,
                    tablegroup_name_,
                    if_exist_);

bool ObAlterTablegroupArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !tablegroup_name_.empty();
}

bool ObAlterTablegroupArg::is_alter_partitions() const
{
  return alter_option_bitset_.has_member(ADD_PARTITION)
         || alter_option_bitset_.has_member(DROP_PARTITION)
         || alter_option_bitset_.has_member(PARTITIONED_TABLE)
         || alter_option_bitset_.has_member(REORGANIZE_PARTITION)
         || alter_option_bitset_.has_member(SPLIT_PARTITION);
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
      if (alter_option_bitset_.has_member(i)
          && i != PRIMARY_ZONE
          && i != LOCALITY
          && i != FORCE_LOCALITY) {
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
  J_KV(K_(table_items),
       K_(tenant_id),
       K_(tablegroup_name),
       K_(alter_tablegroup_schema));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterTablegroupArg, ObDDLArg),
                    tenant_id_,
                    tablegroup_name_,
                    table_items_,
                    alter_option_bitset_,
                    alter_tablegroup_schema_);

OB_SERIALIZE_MEMBER((ObSecurityAuditArg, ObDDLArg),
                    tenant_id_,
                    modify_type_,
                    audit_type_,
                    operation_types_,
                    stmt_user_ids_,
                    obj_object_id_,
                    by_type_,
                    when_type_);

bool ObCreateVertialPartitionArg::is_valid() const
{
  return !vertical_partition_columns_.empty();
}

DEF_TO_STRING(ObCreateVertialPartitionArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(vertical_partition_columns));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateVertialPartitionArg, ObDDLArg),
                    vertical_partition_columns_);

bool ObCreateTableArg::is_valid() const
{
  // index_arg_list can be empty
  return OB_INVALID_ID != schema_.get_tenant_id()
      && !schema_.get_table_name_str().empty();
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
       K_(vertical_partition_arg_list),
       K_(error_info),
       K_(is_alter_view),
       K_(sequence_ddl_arg),
       K_(dep_infos));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateTableArg, ObDDLArg),
                    if_not_exist_,
                    schema_,
                    index_arg_list_,
                    db_name_,
                    foreign_key_arg_list_,
                    constraint_list_,
                    last_replay_log_id_,
                    is_inner_,
                    vertical_partition_arg_list_,
                    error_info_,
                    is_alter_view_,
                    sequence_ddl_arg_,
                    dep_infos_);

bool ObCreateTableArg::is_allow_when_upgrade() const
{
  bool bret = true;
  if (0 != foreign_key_arg_list_.count()
      || 0 != vertical_partition_arg_list_.count()) {
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
  return OB_INVALID_ID != tenant_id_ && !origin_db_name_.empty()
      && !origin_table_name_.empty() && !new_db_name_.empty()
      && !new_table_name_.empty()
      && (table_type_ == USER_TABLE || table_type_ == TMP_TABLE_ORA_SESS || table_type_ == TMP_TABLE);
}

int ObCreateTableLikeArg::assign(const ObCreateTableLikeArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("assign ddl arg failed", K(ret));
  } else if (OB_FAIL(sequence_ddl_arg_.assign(other.sequence_ddl_arg_))) {
    LOG_WARN("assign ddl arg failed", K(ret));
  } else {
    if_not_exist_ = other.if_not_exist_;
    tenant_id_ = other.tenant_id_;
    table_type_ = other.table_type_;
    origin_db_name_ = other.origin_db_name_;
    origin_table_name_ = other.origin_table_name_;
    new_db_name_ = other.new_db_name_;
    new_table_name_ = other.new_table_name_;
    create_host_ = other.create_host_;
    session_id_ = other.session_id_;
    define_user_id_ = other.define_user_id_;
  }
  return ret;
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
       K_(create_host),
       K_(sequence_ddl_arg),
       K_(session_id),
       K_(define_user_id));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateTableLikeArg, ObDDLArg),
                    if_not_exist_,
                    tenant_id_,
                    origin_db_name_,
                    origin_table_name_,
                    new_db_name_,
                    new_table_name_,
                    table_type_,
                    create_host_,
                    sequence_ddl_arg_,
                    session_id_,
                    define_user_id_);

bool ObAlterTableArg::is_valid() const
{
  // TODO(shaohang.lsh): add more check if needed
  if (is_refresh_sess_active_time()) {
    return true;
  } else {
    return OB_INVALID_ID != alter_table_schema_.get_tenant_id()
        && !alter_table_schema_.origin_database_name_.empty()
        && !alter_table_schema_.origin_table_name_.empty();
  }
}

bool ObAlterTableArg::is_refresh_sess_active_time() const
{
  return (alter_table_schema_.alter_option_bitset_.has_member(SESSION_ACTIVE_TIME)
          && OB_DDL_ALTER_TABLE == alter_table_schema_.alter_type_
          && OB_INVALID_ID != session_id_);
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
  if (alter_table_schema_.alter_option_bitset_.is_empty()
      && !is_alter_columns_
      && !is_alter_indexs_) {
    bret = false;
  } else {
    bret = true;
    if (is_alter_indexs_) {
      for (int64_t i = 0 ; bret && i < index_arg_list_.count(); i++) {
        if (OB_ISNULL(index_arg_list_.at(i))) {
          bret = false;
          LOG_WARN_RET(OB_ERR_UNEXPECTED, "ptr is null", K(bret));
        } else {
          bret = index_arg_list_.at(i)->is_allow_when_upgrade();
        }
      }
    }
    for (int64_t i = 0; i < MAX_OPTION && bret && is_alter_options_; i++) {
      if (alter_table_schema_.alter_option_bitset_.has_member(i)
          && i != PRIMARY_ZONE
          && i != LOCALITY
          && i != FORCE_LOCALITY) {
        bret = false;
      }
    }
    if (is_alter_columns_ && bret) {
      // Only add columns and extend the length of the columns will be allowed again in ddl_service
      ObTableSchema::const_column_iterator it_begin = alter_table_schema_.column_begin();
      ObTableSchema::const_column_iterator it_end = alter_table_schema_.column_end();
      AlterColumnSchema *alter_column_schema = NULL;
      for(; bret && it_begin != it_end; it_begin++) {
        if (OB_ISNULL(*it_begin)) {
          bret = false;
          LOG_WARN_RET(OB_ERR_UNEXPECTED, "*it_begin is NULL", K(bret));
        } else {
          alter_column_schema = static_cast<AlterColumnSchema *>(*it_begin);
          // mysql mode, OB_ALL_MODIFY_COLUMN function is a subset of OB_ALL_CHANGE_COLUMN;
          // Oracle mode, only OB_ALL_MODIFY_COLUMN. In the case of only supporting extended column length, for simplicity of implementation, only OB_ALL_MODIFY_COLUMN is left here.
          if (OB_DDL_MODIFY_COLUMN != alter_column_schema->alter_type_
              && OB_DDL_ADD_COLUMN != alter_column_schema->alter_type_) {
            bret = false;
          }
        }
      }
    }
  }
  return bret;
}

int ObAlterTableArg::set_nls_formats(const common::ObString *nls_formats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(nls_formats)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; ++i) {
      if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(nls_formats[i].length()))) {
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

int ObAlterTableArg::serialize_index_args(char *buf, const int64_t data_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (!is_valid() || NULL == buf || data_len <= 0 || pos >= data_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this, KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, data_len, pos, index_arg_list_.size()))) {
    SHARE_LOG(WARN, "Fail to serialize index arg count", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < index_arg_list_.size(); ++i) {
    ObIndexArg *index_arg = index_arg_list_.at(i);
    if (index_arg->index_action_type_ == ObIndexArg::ALTER_PRIMARY_KEY
      || index_arg->index_action_type_ == ObIndexArg::DROP_PRIMARY_KEY) {
      ObAlterPrimaryArg *alter_pk_arg = static_cast<ObAlterPrimaryArg *>(index_arg);
      if (NULL == alter_pk_arg) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos,
          alter_pk_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(alter_pk_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize create index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::ADD_INDEX
              || index_arg->index_action_type_ == ObIndexArg::ADD_PRIMARY_KEY) {
      ObCreateIndexArg *create_index_arg = static_cast<ObCreateIndexArg *>(index_arg);
      if (NULL == create_index_arg) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos,
          create_index_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(create_index_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize create index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::DROP_INDEX) {
      ObDropIndexArg *drop_index_arg = static_cast<ObDropIndexArg *>(index_arg);
      if (NULL == drop_index_arg) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos,
                                                    drop_index_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(drop_index_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize drop index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::ALTER_INDEX) {
      ObAlterIndexArg *alter_index_arg = static_cast<ObAlterIndexArg *>(index_arg);
      if (OB_UNLIKELY(NULL == alter_index_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos,
                                                    alter_index_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(alter_index_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize alter index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::ALTER_INDEX_PARALLEL) {
      ObAlterIndexParallelArg *alter_index_parallel_arg = static_cast<ObAlterIndexParallelArg *>(index_arg);
      if (OB_UNLIKELY(NULL == alter_index_parallel_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos,
                                                    alter_index_parallel_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(alter_index_parallel_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize alter index parallel arg!",
          K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::RENAME_INDEX) {
      ObRenameIndexArg *rename_index_arg = static_cast<ObRenameIndexArg *>(index_arg);
      SHARE_LOG(WARN, "serialize rename index arg!", K(rename_index_arg->origin_index_name_), K(rename_index_arg->new_index_name_));

      if (OB_UNLIKELY(NULL == rename_index_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos,
                                                    rename_index_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(rename_index_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize alter index arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::DROP_FOREIGN_KEY) {
      ObDropForeignKeyArg *foreign_key_arg = static_cast<ObDropForeignKeyArg *>(index_arg);
      if (OB_UNLIKELY(NULL == foreign_key_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos,
                                                    foreign_key_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(foreign_key_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize drop foreign key arg!", K(data_len), K(pos), K(ret));
      }
    } else if (index_arg->index_action_type_ == ObIndexArg::ALTER_INDEX_TABLESPACE) {
      ObAlterIndexTablespaceArg *alter_index_tablespace_arg = static_cast<ObAlterIndexTablespaceArg *>(index_arg);
      if (OB_UNLIKELY(NULL == alter_index_tablespace_arg)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "index arg is null", K(ret));
      } else if (OB_FAIL(serialization::encode_vi32(buf, data_len, pos,
                                                    alter_index_tablespace_arg->index_action_type_))) {
        SHARE_LOG(WARN, "failed to serialize index type", K(ret));
      } else if (OB_FAIL(alter_index_tablespace_arg->serialize(buf, data_len, pos))) {
        SHARE_LOG(WARN, "failed to serialize alter index tablespace arg!",
          K(data_len), K(pos), K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "unknown index action type", K_(index_arg->index_action_type), K(ret));
    }
  }
  return ret;
}

int ObAlterTableArg::alloc_index_arg(const ObIndexArg::IndexActionType index_action_type, ObIndexArg *&index_arg)
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = nullptr;
  if (index_action_type == ObIndexArg::ALTER_PRIMARY_KEY
    || index_action_type == ObIndexArg::DROP_PRIMARY_KEY) {
    ObAlterPrimaryArg *alter_pk_arg = NULL;
    if (NULL == (tmp_ptr = allocator_.alloc(sizeof(ObAlterPrimaryArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "failed to alloc memory!", K(ret));
    } else {
      index_arg = new (tmp_ptr) ObAlterPrimaryArg();
    }
  } else if (index_action_type == ObIndexArg::ADD_INDEX
            || index_action_type == ObIndexArg::ADD_PRIMARY_KEY) {
    ObCreateIndexArg *create_index_arg = NULL;
    if (NULL == (tmp_ptr = allocator_.alloc(sizeof(ObCreateIndexArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "failed to alloc memory!", K(ret));
    } else {
      index_arg = new (tmp_ptr) ObCreateIndexArg();
    }
  } else if (index_action_type == ObIndexArg::DROP_INDEX) {
    ObDropIndexArg *drop_index_arg = NULL;
    if (NULL == (tmp_ptr = (ObDropIndexArg *)allocator_.alloc(sizeof(ObDropIndexArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
    } else {
      index_arg = new (tmp_ptr) ObDropIndexArg();
    }
  } else if (index_action_type == ObIndexArg::ALTER_INDEX) {
    ObAlterIndexArg *alter_index_arg = NULL;
    if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexArg *)allocator_.alloc(sizeof(ObAlterIndexArg))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
    } else {
      index_arg = new (tmp_ptr) ObAlterIndexArg();
    }
  } else if (index_action_type == ObIndexArg::ALTER_INDEX_PARALLEL) {
    ObAlterIndexParallelArg *alter_index_parallel_arg = NULL;
    if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexParallelArg *)allocator_.alloc(sizeof(ObAlterIndexParallelArg))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
    } else {
      index_arg = new (tmp_ptr) ObAlterIndexParallelArg();
    }
  } else if (index_action_type == ObIndexArg::RENAME_INDEX) {
    ObRenameIndexArg *rename_index_arg = NULL;
    if (OB_UNLIKELY(NULL == (tmp_ptr = (ObRenameIndexArg *)allocator_.alloc(sizeof(ObRenameIndexArg))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
    } else {
      index_arg = new (tmp_ptr) ObRenameIndexArg();
    }
  } else if (index_action_type == ObIndexArg::DROP_FOREIGN_KEY) {
    ObDropForeignKeyArg *drop_foreign_key_arg = NULL;
    if (OB_UNLIKELY(NULL == (tmp_ptr = (ObDropForeignKeyArg *)allocator_.alloc(sizeof(ObDropForeignKeyArg))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
    } else {
      index_arg = new (tmp_ptr) ObDropForeignKeyArg();
    }
  } else if (index_action_type == ObIndexArg::ALTER_INDEX_TABLESPACE) {
    if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexTablespaceArg *)allocator_.alloc(sizeof(ObAlterIndexTablespaceArg))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "failed to allocate memory!", K(ret));
    } else {
      index_arg = new (tmp_ptr) ObAlterIndexTablespaceArg();
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "unknown index action type", K(index_action_type), K(ret));
  }
  return ret;
}

int ObAlterTableArg::deserialize_index_args(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    SHARE_LOG(WARN, "Fail to decode column count", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObIndexArg::IndexActionType index_action_type = ObIndexArg::INVALID_ACTION;
    ObIndexArg *index_arg = nullptr;
    if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t *)(&index_action_type))))) {
      SHARE_LOG(WARN, "failed to decode index action type", K(ret));
      break;
    } else if (OB_FAIL(alloc_index_arg(index_action_type, index_arg))) {
      SHARE_LOG(WARN, "alloc index arg failed", K(ret));
    } else if (OB_ISNULL(index_arg)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "error unexpected, index arg must not be nullptr", K(ret));
    } else if (OB_FAIL(index_arg->deserialize(buf, data_len, pos))) {
      SHARE_LOG(WARN, "deserialize index arg failed", K(ret));
    } else if (OB_FAIL(index_arg_list_.push_back(index_arg))) {
      SHARE_LOG(WARN, "push back index arg failed", K(ret));
    }
    if (OB_FAIL(ret) && nullptr != index_arg) {
      index_arg->~ObIndexArg();
      allocator_.free(index_arg);
      index_arg = nullptr;
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
    ObIndexArg *index_arg = index_arg_list_.at(i);
    if (NULL == index_arg) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "index arg should not be null", K(ret));
    } else {
      len += serialization::encoded_length(index_arg->index_action_type_);
      if (ObIndexArg::DROP_PRIMARY_KEY == index_arg->index_action_type_
        || ObIndexArg::ALTER_PRIMARY_KEY == index_arg->index_action_type_) {
        ObAlterPrimaryArg *alter_pk_arg = static_cast<ObAlterPrimaryArg *>(index_arg);
        if (NULL == alter_pk_arg) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += alter_pk_arg->get_serialize_size();
        }
      } else if (ObIndexArg::ADD_INDEX == index_arg->index_action_type_
                || ObIndexArg::ADD_PRIMARY_KEY == index_arg->index_action_type_) {
        ObCreateIndexArg *create_index_arg = static_cast<ObCreateIndexArg *>(index_arg);
        if (NULL == create_index_arg) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += create_index_arg->get_serialize_size();
        }
      } else if (ObIndexArg::DROP_INDEX == index_arg->index_action_type_) {
        ObDropIndexArg *drop_index_arg = static_cast<ObDropIndexArg *>(index_arg);
        if (NULL == drop_index_arg) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += drop_index_arg->get_serialize_size();
        }
      } else if (ObIndexArg::ALTER_INDEX == index_arg->index_action_type_) {
        ObAlterIndexArg *alter_index_arg = static_cast<ObAlterIndexArg *>(index_arg);
        if (OB_UNLIKELY(NULL == alter_index_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += alter_index_arg->get_serialize_size();
        }
      } else if (ObIndexArg::DROP_FOREIGN_KEY == index_arg->index_action_type_) {
        ObDropForeignKeyArg *drop_foreign_key_arg = static_cast<ObDropForeignKeyArg *>(index_arg);
        if (OB_UNLIKELY(NULL == drop_foreign_key_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "foreign key arg is null", K(ret));
        } else {
          len += drop_foreign_key_arg->get_serialize_size();
        }
      } else if (ObIndexArg::ALTER_INDEX_PARALLEL == index_arg->index_action_type_) {
        ObAlterIndexParallelArg *alter_index_parallel_arg =
          static_cast<ObAlterIndexParallelArg *>(index_arg);
        if (OB_UNLIKELY(NULL == alter_index_parallel_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += alter_index_parallel_arg->get_serialize_size();
        }
      } else if (ObIndexArg::RENAME_INDEX == index_arg->index_action_type_) {
        ObRenameIndexArg *rename_index_arg = static_cast<ObRenameIndexArg *>(index_arg);
        if (OB_UNLIKELY(NULL == rename_index_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += rename_index_arg->get_serialize_size();
        }
      } else if (ObIndexArg::ALTER_INDEX_TABLESPACE == index_arg->index_action_type_) {
        ObAlterIndexTablespaceArg *alter_index_tablespace_arg =
          static_cast<ObAlterIndexTablespaceArg *>(index_arg);
        if (OB_UNLIKELY(NULL == alter_index_tablespace_arg)) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "index arg is null", K(ret));
        } else {
          len += alter_index_tablespace_arg->get_serialize_size();
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
  } else if (OB_FAIL(serialize_index_args(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize index args", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(alter_table_schema_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize alter table schema", K(ret));
  } else if (OB_FAIL(tz_info_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize timezone info", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, alter_part_type_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize alter_part_type", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, alter_constraint_type_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize alter_constraint_type", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, session_id_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize session_id", K(ret));
  } else if (OB_FAIL(tz_info_wrap_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize timezone info wrap", K(ret));
  } else {
    for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
      if (OB_FAIL(nls_formats_[i].serialize(buf, buf_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to serialize nls_formats_[i]", K(nls_formats_[i]), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(foreign_key_arg_list_.serialize(buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to serialize foreign_key_arg_list_", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sequence_ddl_arg_.serialize(buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to serialize sequence_ddl_arg_", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, sql_mode_))) {
      SHARE_SCHEMA_LOG(WARN, "fail to serialize sql mode", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
      ddl_task_type_,
      compat_mode_,
      table_id_,
      hidden_table_id_,
      is_alter_columns_,
      is_alter_indexs_,
      is_alter_options_,
      is_alter_partitions_,
      is_inner_,
      is_update_global_indexes_,
      is_convert_to_character_,
      skip_sys_table_check_,
      need_rebuild_trigger_,
      foreign_key_checks_,
      is_add_to_scheduler_,
      inner_sql_exec_addr_);

  return ret;
}

OB_DEF_DESERIALIZE(ObAlterTableArg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize DDLArg", K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(deserialize_index_args(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize index args, ", K(ret));
  } else if (OB_FAIL(alter_table_schema_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter table schema, ", K(ret));
  } else if (OB_FAIL(tz_info_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize timezone info", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t *)(&alter_part_type_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_part_type_, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t *)(&alter_constraint_type_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_constraint_type_, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, ((int64_t *)(&session_id_))))) {
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
    ObString tmp_string;
    char *tmp_ptr[ObNLSFormatEnum::NLS_MAX] = {};
    for (int64_t i = 0; OB_SUCC(ret) && i < ObNLSFormatEnum::NLS_MAX; ++i) {
      if (OB_FAIL(tmp_string.deserialize(buf, data_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to deserialize nls_formats_", K(i), K(ret));
      } else if (OB_ISNULL(tmp_ptr[i] = (char *)allocator_.alloc(tmp_string.length()))) {
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
    if (OB_FAIL(foreign_key_arg_list_.deserialize(buf, data_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to deserialize foreign_key_arg_list_", K(ret));
    }
  }

  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(sequence_ddl_arg_.deserialize(buf, data_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to deserialize sequence_ddl_arg_", K(ret));
    }
  }
  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&sql_mode_)))) {
      SHARE_SCHEMA_LOG(WARN, "fail to decode sql mode", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
      ddl_task_type_,
      compat_mode_,
      table_id_,
      hidden_table_id_,
      is_alter_columns_,
      is_alter_indexs_,
      is_alter_options_,
      is_alter_partitions_,
      is_inner_,
      is_update_global_indexes_,
      is_convert_to_character_,
      skip_sys_table_check_,
      need_rebuild_trigger_,
      foreign_key_checks_,
      is_add_to_scheduler_,
      inner_sql_exec_addr_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObAlterTableArg)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else  {
    len += ObDDLArg::get_serialize_size();
    len += get_index_args_serialize_size();
    len += alter_table_schema_.get_serialize_size();
    len += tz_info_.get_serialize_size();
    len += serialization::encoded_length_vi32(alter_part_type_);
    len += serialization::encoded_length_vi32(alter_constraint_type_);
    len += serialization::encoded_length_vi64(session_id_);
    len += tz_info_wrap_.get_serialize_size();
    for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
      len += nls_formats_[i].get_serialize_size();
    }
    len += foreign_key_arg_list_.get_serialize_size();
    len += sequence_ddl_arg_.get_serialize_size();
    len += serialization::encoded_length_i64(sql_mode_);
    LST_DO_CODE(OB_UNIS_ADD_LEN,
        ddl_task_type_,
        compat_mode_,
        table_id_,
        hidden_table_id_,
        is_alter_columns_,
        is_alter_indexs_,
        is_alter_options_,
        is_alter_partitions_,
        is_inner_,
        is_update_global_indexes_,
        is_convert_to_character_,
        skip_sys_table_check_,
        need_rebuild_trigger_,
        foreign_key_checks_,
        is_add_to_scheduler_,
        inner_sql_exec_addr_);
  }

  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

bool ObTruncateTableArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !database_name_.empty()
      && !table_name_.empty() && lib::Worker::CompatMode::INVALID != compat_mode_;
}

OB_SERIALIZE_MEMBER((ObTruncateTableArg, ObDDLArg),
                    tenant_id_,
                    database_name_,
                    table_name_,
                    session_id_,
                    is_add_to_scheduler_,
                    compat_mode_,
                    foreign_key_checks_);

DEF_TO_STRING(ObTruncateTableArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
       K_(database_name),
       K_(table_name),
       K_(session_id),
       K_(is_add_to_scheduler),
       K_(compat_mode),
       K_(foreign_key_checks));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObRenameTableItem, origin_db_name_,
                                       new_db_name_,
                                       origin_table_name_,
                                       new_table_name_);

DEF_TO_STRING(ObRenameTableItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(origin_db_name),
       K_(new_db_name),
       K_(origin_table_name),
       K_(new_table_name),
       K_(origin_table_id));
  J_OBJ_END();
  return pos;
}

bool ObRenameTableItem::is_valid() const
{
  return !origin_db_name_.empty() && !new_db_name_.empty()
      && !origin_table_name_.empty() && !new_table_name_.empty();
}

bool ObRenameTableArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && rename_table_items_.count() > 0;
}

DEF_TO_STRING(ObRenameTableArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
       K_(rename_table_items));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObRenameTableArg, ObDDLArg),
                    tenant_id_,
                    rename_table_items_);

DEF_TO_STRING(ObTableItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(database_name),
       K_(table_name),
       K_(is_hidden));
  J_OBJ_END();
  return pos;
}

bool ObTableItem::operator==(const ObTableItem &r) const
{
  bool ret = false;
  if (OB_NAME_CASE_INVALID != mode_ && mode_ == r.mode_) {
    if (!table_name_.empty() && !r.table_name_.empty() &&
        !database_name_.empty() && !r.database_name_.empty()) {
      //todo compare using case mode @hualong
      //ret = ObCharset::case_mode_equal(mode_, table_name_, r.table_name_) &&
      //    ObCharset::case_mode_equal(mode_, database_name_, r.database_name_);
      ret = table_name_ == r.table_name_ && database_name_ == r.database_name_ && is_hidden_ == r.is_hidden_;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTableItem,
                    database_name_,
                    table_name_,
                    is_hidden_);

bool ObDropTableArg::is_valid() const
{
  bool ret = (OB_INVALID_ID != tenant_id_ && table_type_ < MAX_TABLE_TYPE
              && tables_.count() > 0);
  if (false == ret && (TMP_TABLE == table_type_
                       || TMP_TABLE_ORA_TRX == table_type_
                       || TMP_TABLE_ORA_SESS == table_type_
                       || TMP_TABLE_ALL == table_type_)) {
    LOG_WARN("drop table valid check for temp table");
    ret = (session_id_ != OB_INVALID_ID && true == if_exist_ && false == to_recyclebin_ && lib::Worker::CompatMode::INVALID != compat_mode_);
  }
  return ret;
}

int ObDropTableArg::assign(const ObDropTableArg &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    //do nothing
  } else if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("assign failed", K(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    session_id_ = other.session_id_;
    sess_create_time_ = other.sess_create_time_;
    table_type_ = other.table_type_;
    if_exist_ = other.if_exist_;
    to_recyclebin_ = other.to_recyclebin_;
    for (int64_t i = 0; OB_SUCC(ret) && i < other.tables_.count(); i++) {
      ObTableItem table_item;
      table_item.mode_ = other.tables_.at(i).mode_;
      OZ (ob_write_string(allocator_, other.tables_.at(i).table_name_, table_item.table_name_));
      OZ (ob_write_string(allocator_, other.tables_.at(i).database_name_, table_item.database_name_));
      OZ (tables_.push_back(table_item));
    }
  }
  return ret;
}

DEF_TO_STRING(ObDropTableArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
       K_(table_type),
       K_(tables),
       K_(if_exist),
       K_(to_recyclebin),
       K_(session_id),
       K_(sess_create_time),
       K_(foreign_key_checks),
       K_(is_add_to_scheduler),
       K_(force_drop),
       K_(compat_mode));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObDropTableArg, ObDDLArg),
                    tenant_id_,
                    table_type_,
                    tables_,
                    if_exist_,
                    to_recyclebin_,
                    session_id_,
                    sess_create_time_,
                    foreign_key_checks_,
                    is_add_to_scheduler_,
                    force_drop_,
                    compat_mode_);

bool ObOptimizeTableArg::is_valid() const
{
  return (OB_INVALID_ID != tenant_id_ && tables_.count() > 0);
}

OB_SERIALIZE_MEMBER((ObOptimizeTableArg, ObDDLArg),
    tenant_id_, tables_);

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
  J_KV(K_(column_name),
       K_(prefix_len),
       K_(order_type),
       K_(column_id),
       K_(is_func_index));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObColumnSortItem,
                                 column_name_,
                                 prefix_len_,
                                 order_type_,
                                 column_id_,
                                 is_func_index_);

bool ObTableOption::is_valid() const
{
  // if replica_num not set, it's default value is zero
  return block_size_ > 0 && replica_num_ >= 0
      && index_status_ > INDEX_STATUS_NOT_FOUND && index_status_ < INDEX_STATUS_MAX
      && !compress_method_.empty() && progressive_merge_num_ >= 0
      && ObStoreFormat::is_store_format_valid(store_format_)
      && ObStoreFormat::is_row_store_type_valid(row_store_type_);
}

bool ObIndexOption::is_valid() const
{
  // if replica_num not set, it's default value is zero
  return block_size_ > 0
      && index_status_ > INDEX_STATUS_NOT_FOUND
      && index_status_ < INDEX_STATUS_MAX
      && progressive_merge_num_ >= 0;
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

OB_SERIALIZE_MEMBER(ObTableOption,
                                 block_size_,
                                 replica_num_,
                                 index_status_,
                                 use_bloom_filter_,
                                 compress_method_,
                                 comment_,
                                 progressive_merge_num_,
                                 row_store_type_,
                                 store_format_);

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
  return OB_INVALID_ID != tenant_id_ && !index_name_.empty() && !table_name_.empty()
      && !database_name_.empty() && INVALID_ACTION != index_action_type_;
}

bool ObIndexArg::is_allow_when_upgrade() const
{
  return ADD_INDEX == index_action_type_
         || DROP_INDEX == index_action_type_
         || DROP_FOREIGN_KEY == index_action_type_;
}

DEF_TO_STRING(ObIndexArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
       K_(session_id),
       K_(index_name),
       K_(table_name),
       K_(database_name),
       K_(index_action_type));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObIndexArg, ObDDLArg),
                    tenant_id_,
                    index_name_,
                    table_name_,
                    database_name_,
                    index_action_type_,
                    session_id_);

bool ObCreateIndexArg::is_valid() const
{
  // store_columns_ can be empty
  return ObIndexArg::is_valid() && index_type_ > INDEX_TYPE_IS_NOT
         && index_type_ < INDEX_TYPE_MAX
         && index_columns_.count() > 0
         && index_option_.is_valid()
         && index_using_type_ >= USING_BTREE
         && index_using_type_ < USING_TYPE_MAX;
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
       K_(sql_mode),
       K_(inner_sql_exec_addr));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateIndexArg, ObIndexArg),
                    index_type_,
                    index_columns_,
                    store_columns_,
                    index_option_,
                    index_using_type_,
                    fulltext_columns_,
                    data_table_id_,
                    index_table_id_,
                    if_not_exist_,
                    with_rowid_,
                    index_schema_,
                    is_inner_,
                    hidden_store_columns_,
                    nls_date_format_,
                    nls_timestamp_format_,
                    nls_timestamp_tz_format_,
                    sql_mode_,
                    inner_sql_exec_addr_);

bool ObAlterIndexArg::is_valid() const
{
  // store_columns_ can be empty
  return (ObIndexArg::is_valid() && (0 == index_visibility_ || 1 == index_visibility_ ));
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

DEF_TO_STRING(ObDropIndexArg) {
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
       K_(index_name),
       K_(table_name),
       K_(database_name),
       K_(index_action_type),
       K_(index_table_id),
       K_(is_add_to_scheduler),
       K_(is_in_recyclebin),
       K_(is_hidden),
       K_(is_inner));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER((ObDropIndexArg, ObIndexArg),
                    tenant_id_,
                    index_name_,
                    table_name_,
                    database_name_,
                    index_action_type_,
                    index_table_id_,
                    is_add_to_scheduler_,
                    is_in_recyclebin_,
                    is_hidden_,
                    is_inner_);

OB_SERIALIZE_MEMBER(ObDropIndexRes, tenant_id_, index_table_id_, schema_version_, task_id_);

int ObDropIndexRes::assign(const ObDropIndexRes &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  index_table_id_ = other.index_table_id_;
  schema_version_ = other.schema_version_;
  task_id_ = other.task_id_;
  return ret;
}

DEF_TO_STRING(ObRebuildIndexArg) {
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id),
       K_(index_name),
       K_(table_name),
       K_(database_name),
       K_(index_action_type),
       K_(index_table_id));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER((ObRebuildIndexArg, ObIndexArg),
                    index_table_id_);

bool ObRenameIndexArg::is_valid() const
{
  int bret = true;
  if (origin_index_name_.empty()) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "origin_index_name is empty", K_(origin_index_name));
  } else if (new_index_name_.empty()){
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "new_index_name is empty", K_(origin_index_name));
  }else{
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
  J_KV(K_(origin_index_name),
       K_(new_index_name));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObRenameIndexArg, ObIndexArg), origin_index_name_, new_index_name_);

bool ObCreateForeignKeyArg::is_valid() const
{
  return ObIndexArg::is_valid() && !parent_table_.empty()
      && child_columns_.count() > 0 && parent_columns_.count() > 0 && child_columns_.count() == parent_columns_.count()
      && ACTION_INVALID < update_action_ && update_action_ < ACTION_MAX
      && ACTION_INVALID < delete_action_ && delete_action_ < ACTION_MAX;
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
       K_(is_modify_fk_state),
       K_(parent_database_id),
       K_(parent_table_id),
       K_(name_generated_type));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObCreateForeignKeyArg, ObIndexArg),
                    parent_database_,
                    parent_table_,
                    child_columns_,
                    parent_columns_,
                    update_action_,
                    delete_action_,
                    foreign_key_name_,
                    enable_flag_,
                    is_modify_enable_flag_,
                    ref_cst_type_,
                    ref_cst_id_,
                    validate_flag_,
                    is_modify_validate_flag_,
                    rely_flag_,
                    is_modify_rely_flag_,
                    is_modify_fk_state_,
                    need_validate_data_,
                    is_parent_table_mock_,
                    parent_database_id_,
                    parent_table_id_,
                    name_generated_type_);

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

OB_SERIALIZE_MEMBER((ObDropForeignKeyArg, ObIndexArg),
                    foreign_key_name_);

DEF_TO_STRING(ObAlterIndexTablespaceArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME(N_INDEX_ARG);
  J_COLON();
  pos += ObIndexArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(tablespace_id), K_(encryption));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObAlterIndexTablespaceArg, ObIndexArg), tablespace_id_, encryption_);

bool ObFlashBackTableFromRecyclebinArg::is_valid() const
{
  int bret = true;
  if (OB_INVALID_ID == tenant_id_) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "tenant_id is invalid", K_(tenant_id));
  } else if (origin_table_name_.empty()) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "origin_table_name is empty", K_(origin_table_name));
  } else if ((new_db_name_.empty() && !new_table_name_.empty()) ||
      (!new_db_name_.empty() && new_table_name_.empty())) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "new_db_name or new_table_name is invalid",
             K_(new_db_name), K_(new_table_name));
  }
  return bret;
}

OB_SERIALIZE_MEMBER((ObFlashBackTableFromRecyclebinArg, ObDDLArg),
                    tenant_id_,
                    origin_table_name_,
                    new_db_name_,
                    new_table_name_,
                    origin_db_name_);

bool ObFlashBackTableToScnArg::is_valid() const
{
  int bret = true;
  if (OB_INVALID_ID == tenant_id_) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "tenant_id is invalid", K_(tenant_id));
  } else if (OB_INVALID_ID == time_point_) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "timepoint is invalid", K_(time_point));
  } else if (0 == tables_.count()) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "table is empty", K_(tables));
  } else if (-1 == query_end_time_) {
    bret = false;
  }
  return bret;
}

OB_SERIALIZE_MEMBER(ObFlashBackTableToScnArg,
                    tenant_id_,
                    time_point_,
                    tables_,
                    query_end_time_);

bool ObFlashBackIndexArg::is_valid() const
{
  int bret = true;
  if (OB_INVALID_ID == tenant_id_) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "tenant_id is invalid", K_(tenant_id));
  } else if (origin_table_name_.empty()) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "origin_table_name is empty", K_(origin_table_name));
  } else if ((new_db_name_.empty() && !new_table_name_.empty()) ||
      (!new_db_name_.empty() && new_table_name_.empty())) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ERROR, "new_db_name or new_table_name is invalid",
             K_(new_db_name), K_(new_table_name));
  }
  return bret;
}

OB_SERIALIZE_MEMBER((ObFlashBackIndexArg, ObDDLArg),
                    tenant_id_,
                    origin_table_name_,
                    new_db_name_,
                    new_table_name_);

bool ObPurgeIndexArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != database_id_ && !table_name_.empty();
}

int ObPurgeIndexArg::init(const uint64_t tenant_id,
           const uint64_t exec_tenant_id,
           const ObString &table_name,
           const uint64_t table_id,
           const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || OB_INVALID_TENANT_ID == exec_tenant_id
                  || table_name.empty()
                  || OB_INVALID_ID == table_id
                  || ddl_stmt_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id),
        K(exec_tenant_id), K(table_name), K(table_id), K(ddl_stmt_str));
  } else {
    exec_tenant_id_ = exec_tenant_id;
    tenant_id_ = tenant_id;
    table_name_ = table_name;
    ddl_stmt_str_ = ddl_stmt_str;
    table_id_ = table_id;
  }
  return ret;
}

int ObPurgeIndexArg::assign(const ObPurgeIndexArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ObDDLArg::assign(other))) {
      LOG_WARN("failed to assign other", KR(ret), K(other));
    } else {
      tenant_id_ = other.tenant_id_;
      table_name_ = other.table_name_;
      table_id_ = other.table_id_;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObPurgeIndexArg, ObDDLArg),
                    tenant_id_,
                    database_id_,
                    table_name_);

bool ObFlashBackDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !origin_db_name_.empty();
}

OB_SERIALIZE_MEMBER((ObFlashBackDatabaseArg, ObDDLArg),
                    tenant_id_,
                    origin_db_name_,
                    new_db_name_);

bool ObFlashBackTenantArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER((ObFlashBackTenantArg, ObDDLArg),
                    tenant_id_,
                    origin_tenant_name_,
                    new_tenant_name_);

bool ObPurgeTableArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != database_id_ && !table_name_.empty();
}

OB_SERIALIZE_MEMBER((ObPurgeTableArg, ObDDLArg),
                    tenant_id_,
                    database_id_,
                    table_name_);

bool ObPurgeDatabaseArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ &&
      !db_name_.empty();
}

OB_SERIALIZE_MEMBER((ObPurgeDatabaseArg, ObDDLArg),
                    tenant_id_,
                    db_name_);

bool ObPurgeTenantArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !tenant_name_.empty();
}

OB_SERIALIZE_MEMBER((ObPurgeTenantArg, ObDDLArg),
                    tenant_id_,
                    tenant_name_);

bool ObPurgeRecycleBinArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ || 0 == purge_num_ || 0 == expire_time_;
}

int ObPurgeRecycleBinArg::assign(const ObPurgeRecycleBinArg &other)
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

OB_SERIALIZE_MEMBER((ObPurgeRecycleBinArg, ObDDLArg),
                    tenant_id_,
                    purge_num_,
                    expire_time_,
                    auto_purge_);

OB_SERIALIZE_MEMBER((ObProfileDDLArg, ObDDLArg), schema_, ddl_type_, is_cascade_);

int ObDependencyObjDDLArg::assign(const ObDependencyObjDDLArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    reset_view_column_infos_ = other.reset_view_column_infos_;
    if (OB_FAIL(ObDDLArg::assign(other))) {
      LOG_WARN("fail to assign ddl arg", KR(ret));
    } else if (OB_FAIL(insert_dep_objs_.assign(other.insert_dep_objs_))) {
      LOG_WARN("fail to assign keys", KR(ret), K(other));
    } else if (OB_FAIL(update_dep_objs_.assign(other.update_dep_objs_))) {
      LOG_WARN("fail to assign keys", KR(ret), K(other));
    } else if (OB_FAIL(delete_dep_objs_.assign(other.delete_dep_objs_))) {
      LOG_WARN("fail to assign keys", KR(ret), K(other));
    } else if (OB_FAIL(schema_.assign(other.schema_))) {
      LOG_WARN("fail to assign schema", K(ret));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDependencyObjDDLArg, ObDDLArg),
                    tenant_id_,
                    insert_dep_objs_,
                    update_dep_objs_,
                    delete_dep_objs_,
                    schema_,
                    reset_view_column_infos_);


OB_SERIALIZE_MEMBER(ObCheckFrozenScnArg, frozen_scn_);
OB_SERIALIZE_MEMBER(ObGetMinSSTableSchemaVersionArg, tenant_id_arg_list_);
OB_SERIALIZE_MEMBER(ObGetMinSSTableSchemaVersionRes, ret_list_);

ObCheckFrozenScnArg::ObCheckFrozenScnArg()
{
  frozen_scn_.set_min();
}

bool ObCheckFrozenScnArg::is_valid() const
{
  return frozen_scn_.is_valid() && frozen_scn_ > SCN::min_scn();
}

void ObCreateTabletBatchRes::reset()
{
  ret_ = common::OB_SUCCESS;
}

DEF_TO_STRING(ObCreateTabletBatchRes)
{
  int64_t pos = 0;
  J_KV(K_(ret));
  return pos;
}

OB_SERIALIZE_MEMBER(ObCreateTabletBatchRes, ret_);

void ObCreateTabletBatchInTransRes::reset()
{
  ret_ = common::OB_SUCCESS;
  tx_result_.reset();
}

DEF_TO_STRING(ObCreateTabletBatchInTransRes)
{
  int64_t pos = 0;
  J_KV(K_(ret), K_(tx_result));
  return pos;
}

OB_SERIALIZE_MEMBER(ObCreateTabletBatchInTransRes, ret_, tx_result_);

void ObRemoveTabletRes::reset()
{
  ret_ = common::OB_SUCCESS;
}

DEF_TO_STRING(ObRemoveTabletRes)
{
  int64_t pos = 0;
  J_KV(K_(ret));
  return pos;
}

OB_SERIALIZE_MEMBER(ObRemoveTabletRes, ret_);

OB_SERIALIZE_MEMBER(ObCalcColumnChecksumRequestArg::SingleItem, ls_id_, tablet_id_, calc_table_id_);

bool ObCalcColumnChecksumRequestArg::SingleItem::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && OB_INVALID_ID != calc_table_id_;
}

void ObCalcColumnChecksumRequestArg::SingleItem::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  calc_table_id_ = OB_INVALID_ID;
}

int ObCalcColumnChecksumRequestArg::SingleItem::assign(const SingleItem &other)
{
  int ret = OB_SUCCESS;
  ls_id_ = other.ls_id_;
  tablet_id_ = other.tablet_id_;
  calc_table_id_ = other.calc_table_id_;
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObCalcColumnChecksumRequestArg,
    tenant_id_,
    target_table_id_,
    schema_version_,
    execution_id_,
    snapshot_version_,
    source_table_id_,
    task_id_,
    calc_items_);

bool ObCalcColumnChecksumRequestArg::is_valid() const
{
  bool bret = OB_INVALID_ID != tenant_id_ &&  OB_INVALID_ID != target_table_id_
      && OB_INVALID_VERSION != schema_version_ && execution_id_ >= 0
      && OB_INVALID_VERSION != snapshot_version_ && OB_INVALID_ID != source_table_id_
      && task_id_ > 0;
  for (int64_t i = 0; bret && i < calc_items_.count(); ++i) {
    bret = calc_items_.at(i).is_valid();
  }
  return bret;
}

void ObCalcColumnChecksumRequestArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  target_table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  snapshot_version_ = OB_INVALID_VERSION;
  source_table_id_ = OB_INVALID_ID;
  execution_id_ = -1;
  task_id_ = 0;
}

OB_SERIALIZE_MEMBER(ObCalcColumnChecksumRequestRes, ret_codes_);

OB_SERIALIZE_MEMBER(
    ObCalcColumnChecksumResponseArg,
    tablet_id_,
    target_table_id_,
    ret_code_,
    source_table_id_,
    schema_version_,
    task_id_,
    tenant_id_);

bool ObCalcColumnChecksumResponseArg::is_valid() const
{
  return tablet_id_.is_valid()
      && OB_INVALID_ID != target_table_id_
      && OB_INVALID_ID != source_table_id_
      && schema_version_ > 0
      && task_id_ > 0
      && tenant_id_ != OB_INVALID_TENANT_ID;
}

void ObCalcColumnChecksumResponseArg::reset()
{
  tablet_id_.reset();
  target_table_id_ = OB_INVALID_ID;
  ret_code_ = OB_SUCCESS;
  source_table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  task_id_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

OB_SERIALIZE_MEMBER(ObLSMigrateReplicaArg,
                    task_id_,
                    tenant_id_,
                    ls_id_,
                    src_,
                    dst_,
                    data_source_,
                    paxos_replica_number_,
                    skip_change_member_list_);

int ObLSMigrateReplicaArg::assign(
    const ObLSMigrateReplicaArg &that)
{
  int ret = OB_SUCCESS;
  task_id_ = that.task_id_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  src_ = that.src_;
  dst_ = that.dst_;
  data_source_ = that.data_source_;
  paxos_replica_number_ = that.paxos_replica_number_;
  skip_change_member_list_ = that.skip_change_member_list_;
  return ret;
}

int ObLSMigrateReplicaArg::init(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObReplicaMember &src,
    const common::ObReplicaMember &dst,
    const common::ObReplicaMember &data_source,
    const int64_t paxos_replica_number,
    const bool skip_change_member_list)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  src_ = src;
  dst_ = dst;
  data_source_ = data_source;
  paxos_replica_number_ = paxos_replica_number;
  skip_change_member_list_ = skip_change_member_list;
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSAddReplicaArg,
                    task_id_,
                    tenant_id_,
                    ls_id_,
                    dst_,
                    data_source_,
                    orig_paxos_replica_number_,
                    new_paxos_replica_number_,
                    skip_change_member_list_);

int ObLSAddReplicaArg::assign(
    const ObLSAddReplicaArg &that)
{
  int ret = OB_SUCCESS;
  task_id_ = that.task_id_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  dst_ = that.dst_;
  data_source_ = that.data_source_;
  orig_paxos_replica_number_ = that.orig_paxos_replica_number_;
  new_paxos_replica_number_ = that.new_paxos_replica_number_;
  skip_change_member_list_ = that.skip_change_member_list_;
  return ret;
}

int ObLSAddReplicaArg::init(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObReplicaMember &dst,
    const common::ObReplicaMember &data_source,
    const int64_t orig_paxos_replica_number,
    const int64_t new_paxos_replica_number,
    const bool skip_change_member_list)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  dst_ = dst;
  data_source_ = data_source;
  orig_paxos_replica_number_ = orig_paxos_replica_number;
  new_paxos_replica_number_ = new_paxos_replica_number;
  skip_change_member_list_ = skip_change_member_list;
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSChangeReplicaArg,
                    task_id_,
                    tenant_id_,
                    ls_id_,
                    src_,
                    dst_,
                    data_source_,
                    orig_paxos_replica_number_,
                    new_paxos_replica_number_,
                    skip_change_member_list_);

int ObLSChangeReplicaArg::assign(
    const ObLSChangeReplicaArg &that)
{
  int ret = OB_SUCCESS;
  task_id_ = that.task_id_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  src_ = that.src_;
  dst_ = that.dst_;
  data_source_ = that.data_source_;
  orig_paxos_replica_number_ = that.orig_paxos_replica_number_;
  new_paxos_replica_number_ = that.new_paxos_replica_number_;
  skip_change_member_list_ = that.skip_change_member_list_;
  return ret;
}

int ObLSChangeReplicaArg::init(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObReplicaMember &src,
    const common::ObReplicaMember &dst,
    const common::ObReplicaMember &data_source,
    const int64_t orig_paxos_replica_number,
    const int64_t new_paxos_replica_number,
    const bool skip_change_member_list)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  src_ = src;
  dst_ = dst;
  data_source_ = data_source;
  orig_paxos_replica_number_ = orig_paxos_replica_number;
  new_paxos_replica_number_ = new_paxos_replica_number;
  skip_change_member_list_ = skip_change_member_list;
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSDropPaxosReplicaArg,
                    task_id_,
                    tenant_id_,
                    ls_id_,
                    remove_member_,
                    orig_paxos_replica_number_,
                    new_paxos_replica_number_);

int ObLSDropPaxosReplicaArg::assign(
    const ObLSDropPaxosReplicaArg &that)
{
  int ret = OB_SUCCESS;
  task_id_ = that.task_id_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  remove_member_ = that.remove_member_;
  orig_paxos_replica_number_ = that.orig_paxos_replica_number_;
  new_paxos_replica_number_ = that.new_paxos_replica_number_;
  return ret;
}

int ObLSDropPaxosReplicaArg::init(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObReplicaMember &remove_member,
    const int64_t orig_paxos_replica_number,
    const int64_t new_paxos_replica_number)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  remove_member_ = remove_member;
  orig_paxos_replica_number_ = orig_paxos_replica_number;
  new_paxos_replica_number_ = new_paxos_replica_number;
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSDropNonPaxosReplicaArg,
                    task_id_,
                    tenant_id_,
                    ls_id_,
                    remove_member_);

int ObLSDropNonPaxosReplicaArg::assign(
    const ObLSDropNonPaxosReplicaArg &that)
{
  int ret = OB_SUCCESS;
  task_id_ = that.task_id_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  remove_member_ = that.remove_member_;
  return ret;
}

int ObLSDropNonPaxosReplicaArg::init(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObReplicaMember &remove_member)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  remove_member_ = remove_member;
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSModifyPaxosReplicaNumberArg,
                    task_id_,
                    tenant_id_,
                    ls_id_,
                    orig_paxos_replica_number_,
                    new_paxos_replica_number_,
                    member_list_);

int ObLSModifyPaxosReplicaNumberArg::assign(
    const ObLSModifyPaxosReplicaNumberArg &that)
{
  int ret = OB_SUCCESS;
  task_id_ = that.task_id_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  orig_paxos_replica_number_ = that.orig_paxos_replica_number_;
  new_paxos_replica_number_ = that.new_paxos_replica_number_;
  member_list_ = that.member_list_;
  return ret;
}

int ObLSModifyPaxosReplicaNumberArg::init(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t orig_paxos_replica_number,
    const int64_t new_paxos_replica_number,
    const common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  orig_paxos_replica_number_ = orig_paxos_replica_number;
  new_paxos_replica_number_ = new_paxos_replica_number;
  member_list_ = member_list;
  return ret;
}

OB_SERIALIZE_MEMBER(ObDRTaskReplyResult,
                    task_id_,
                    tenant_id_,
                    ls_id_,
                    result_);

int ObDRTaskReplyResult::assign(
    const ObDRTaskReplyResult &that)
{
  int ret = OB_SUCCESS;
  task_id_ = that.task_id_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  result_ = that.result_;
  return ret;
}

int ObDRTaskReplyResult::init(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int result)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  result_ = result;
  return ret;
}

OB_SERIALIZE_MEMBER(ObDRTaskExistArg,
                    task_id_,
                    tenant_id_,
                    ls_id_);

int ObDRTaskExistArg::assign(
    const ObDRTaskExistArg &that)
{
  int ret = OB_SUCCESS;
  task_id_ = that.task_id_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  return ret;
}

int ObDRTaskExistArg::init(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (task_id.is_invalid() || tenant_id == OB_INVALID_ID || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "failed to assign res_array_", K(ret));
  } else {
    task_id_ = task_id;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
OB_SERIALIZE_MEMBER(ObAddArbArg,
                    tenant_id_,
                    ls_id_,
                    arb_member_,
                    timeout_us_);
int ObAddArbArg::assign(
    const ObAddArbArg &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(arb_member_.assign(that.arb_member_))) {
    SHARE_LOG(WARN, "fail to assign arb member", K(ret));
  } else {
    tenant_id_ = that.tenant_id_;
    ls_id_ = that.ls_id_;
    timeout_us_ = that.timeout_us_;
  }
  return ret;
}
int ObAddArbArg::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObMember &arb_member,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID
      || !ls_id.is_valid()
      || !ls_id.is_valid_with_tenant(tenant_id)
      || !arb_member.is_valid()
      || timeout_us == OB_INVALID_TIMESTAMP) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "failed to init ObAddArbArg", K(ret), K(tenant_id),
              K(ls_id), K(arb_member), K(timeout_us));
  } else if (OB_FAIL(arb_member_.assign(arb_member))) {
    SHARE_LOG(WARN, "fail to assign arb member", K(ret));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    timeout_us_ = timeout_us;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRemoveArbArg,
                    tenant_id_,
                    ls_id_,
                    arb_member_,
                    timeout_us_);

int ObRemoveArbArg::assign(
    const ObRemoveArbArg &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(arb_member_.assign(that.arb_member_))) {
    SHARE_LOG(WARN, "fail to assign arb member", K(ret));
  } else {
    tenant_id_ = that.tenant_id_;
    ls_id_ = that.ls_id_;
    timeout_us_ = that.timeout_us_;
  }
  return ret;
}

int ObRemoveArbArg::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObMember &arb_member,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID
      || !ls_id.is_valid()
      || !ls_id.is_valid_with_tenant(tenant_id)
      || !arb_member.is_valid()
      || timeout_us == OB_INVALID_TIMESTAMP) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "failed to init ObRemoveArbArg", K(ret), K(tenant_id),
              K(ls_id), K(arb_member), K(timeout_us));
  } else if (OB_FAIL(arb_member_.assign(arb_member))) {
    SHARE_LOG(WARN, "fail to assign arb member", K(ret));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    timeout_us_ = timeout_us;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObForceClearArbClusterInfoArg, cluster_id_);
#endif

//----End structs for partition online/offline----

void ObSwitchSchemaArg::reset()
{
  schema_info_.reset();
  force_refresh_ = false;
  is_async_ = false;
}

DEF_TO_STRING(ObSwitchSchemaArg)
{
  int64_t pos = 0;
  J_KV(K_(schema_info),
       K_(force_refresh),
       K_(is_async));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSwitchSchemaArg, schema_info_, force_refresh_, is_async_);

DEF_TO_STRING(ObSwitchLeaderArg)
{
  int64_t pos = 0;
  J_KV(K_(ls_id),
       K_(role),
       K_(tenant_id),
       K_(dest_server));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSwitchLeaderArg, ls_id_, role_, tenant_id_, dest_server_);

OB_SERIALIZE_MEMBER(ObLSTabletPair, ls_id_, tablet_id_);
OB_SERIALIZE_MEMBER(ObCheckSchemaVersionElapsedArg, tenant_id_, schema_version_, need_wait_trans_end_, tablets_);

bool ObCheckSchemaVersionElapsedArg::is_valid() const
{
  bool bret = OB_INVALID_ID != tenant_id_ && schema_version_ > 0 && !tablets_.empty();
  for (int64_t i = 0; bret && i < tablets_.count(); ++i) {
    bret = tablets_.at(i).is_valid();
  }
  return bret;
}

void ObCheckSchemaVersionElapsedArg::reuse()
{
  tenant_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  need_wait_trans_end_ = true;
  tablets_.reuse();
}

bool ObCheckModifyTimeElapsedArg::is_valid() const
{
  bool bret = OB_INVALID_ID != tenant_id_ && sstable_exist_ts_ > 0;
  for (int64_t i = 0; bret && i < tablets_.count(); ++i) {
    bret = tablets_.at(i).is_valid();
  }
  return bret;
}

int ObDDLCheckTabletMergeStatusArg::assign(const ObDDLCheckTabletMergeStatusArg &other) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_ids_.assign(other.tablet_ids_))) {
    LOG_WARN("assign tablet_ids_ failed", K(ret), K(other.tablet_ids_));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    snapshot_version_ = other.snapshot_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLCheckTabletMergeStatusArg, tenant_id_, ls_id_, tablet_ids_, snapshot_version_);

void ObCheckModifyTimeElapsedArg::reuse()
{
  tenant_id_ = OB_INVALID_ID;
  sstable_exist_ts_ = 0;
  tablets_.reuse();
}

OB_SERIALIZE_MEMBER(ObCheckModifyTimeElapsedArg, tenant_id_, sstable_exist_ts_, tablets_);

bool ObCheckSchemaVersionElapsedResult::is_valid() const
{
  bool bret = !results_.empty();
  for (int64_t i = 0; bret && i < results_.count(); ++i) {
    bret = (common::OB_INVALID_TIMESTAMP != results_.at(i).snapshot_);
  }
  return bret;
}

OB_SERIALIZE_MEMBER(ObCheckTransElapsedResult, ret_code_, snapshot_, pending_tx_id_);
OB_SERIALIZE_MEMBER(ObCheckSchemaVersionElapsedResult, results_);


OB_SERIALIZE_MEMBER(CandidateStatus, candidate_status_);

OB_SERIALIZE_MEMBER(ObDDLCheckTabletMergeStatusResult, merge_status_);

//----Structs for managing privileges----
OB_SERIALIZE_MEMBER(ObAccountArg,
                    user_name_,
                    host_name_,
                    is_role_);

bool ObSchemaReviseArg::is_valid() const
{
  bool bret = false;
  if (REVISE_CONSTRAINT_COLUMN_INFO == type_
      || REVISE_NOT_NULL_CONSTRAINT == type_) {
    bret = (OB_INVALID_ID != tenant_id_)
           && (OB_INVALID_ID != table_id_)
           && !(REVISE_CONSTRAINT_COLUMN_INFO == type_ && 0 == csts_array_.count());
  }
  return bret;
}

int ObSchemaReviseArg::assign(const ObSchemaReviseArg &other)
{
  int ret = OB_SUCCESS;
  type_ = other.type_;
  tenant_id_ = other.tenant_id_;
  table_id_ = other.table_id_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(csts_array_.assign(other.csts_array_))) {
    LOG_WARN("failed to assign csts array", KR(ret), K(other));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObSchemaReviseArg, ObDDLArg),
                    type_,
                    tenant_id_,
                    table_id_,
                    csts_array_);

bool ObCreateUserArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && user_infos_.count() > 0;
}

int ObCreateUserArg::assign(const ObCreateUserArg &other)
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

OB_SERIALIZE_MEMBER((ObCreateUserArg, ObDDLArg),
                    tenant_id_,
                    user_infos_,
                    if_not_exist_,
                    creator_id_,
                    primary_zone_);

bool ObDropUserArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && users_.count() > 0 && hosts_.count() == users_.count();
}

OB_DEF_SERIALIZE(ObDropUserArg)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              users_,
              hosts_,
              is_role_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDropUserArg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              users_,
              hosts_);

  //compatibility for old version
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
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              users_,
              hosts_,
              is_role_);
  return len;
}

bool ObAlterRoleArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && role_name_.length() > 0;
}

int ObAlterRoleArg::assign(const ObAlterRoleArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  role_name_ = other.role_name_;
  host_name_ = other.host_name_;
  pwd_enc_ = other.pwd_enc_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  }

  return ret;
}

OB_DEF_SERIALIZE(ObAlterRoleArg)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              role_name_,
              host_name_,
              pwd_enc_);
  return ret;
}

OB_DEF_DESERIALIZE(ObAlterRoleArg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              role_name_,
              host_name_,
              pwd_enc_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObAlterRoleArg)
{
  int64_t len = ObDDLArg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              role_name_,
              host_name_,
              pwd_enc_);
  return len;
}

bool ObRenameUserArg::is_valid() const
{
  return (OB_INVALID_ID != tenant_id_
          && old_users_.count() > 0
          && old_users_.count() == new_users_.count()
          && old_hosts_.count() == new_hosts_.count()
          && old_users_.count() == old_hosts_.count());
}

OB_DEF_SERIALIZE(ObRenameUserArg)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              old_users_,
              new_users_,
              old_hosts_,
              new_hosts_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRenameUserArg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              old_users_,
              new_users_,
              old_hosts_,
              new_hosts_);

  //compatibility for old version
  if (OB_SUCC(ret)
      && old_users_.count() > 0
      && new_users_.count() == old_users_.count()
      && (old_hosts_.empty() || new_hosts_.empty())) {
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
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              old_users_,
              new_users_,
              old_hosts_,
              new_hosts_);
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
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              user_,
              passwd_,
              host_,
              ssl_type_,
              ssl_cipher_,
              x509_issuer_,
              x509_subject_,
              modify_max_connections_,
              max_connections_per_hour_,
              max_user_connections_);
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
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              user_,
              passwd_,
              host_,
              ssl_type_,
              ssl_cipher_,
              x509_issuer_,
              x509_subject_,
              modify_max_connections_,
              max_connections_per_hour_,
              max_user_connections_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSetPasswdArg)
{
  int64_t len = ObDDLArg::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              user_,
              passwd_,
              host_,
              ssl_type_,
              ssl_cipher_,
              x509_issuer_,
              x509_subject_,
              modify_max_connections_,
              max_connections_per_hour_,
              max_user_connections_);
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
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              users_,
              locked_,
              hosts_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLockUserArg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((, ObDDLArg));
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              users_,
              locked_,
              hosts_);

  //compatibility for old version
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
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              users_,
              locked_,
              hosts_);
  return len;
}

int ObAlterUserProfileArg::assign(const ObAlterUserProfileArg &other)
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
  return is_valid_tenant_id(tenant_id_)
      && ((user_name_.length() > 0
          && host_name_.length() > 0)
         || is_valid_id(user_id_)) ;
}

OB_SERIALIZE_MEMBER((ObAlterUserProfileArg, ObDDLArg),
                    tenant_id_,
                    user_name_,
                    host_name_,
                    profile_name_,
                    user_id_,
                    default_role_flag_,
                    role_id_array_);

bool ObGrantArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         /* Oracle mode不一样的权限体系
          * && priv_level_ > OB_PRIV_INVALID_LEVEL
         && priv_level_ < OB_PRIV_MAX_LEVEL
         && users_passwd_.count() > 0
         && users_passwd_.count() == hosts_.count() * 2
         */;
}

bool ObGrantArg::is_allow_when_disable_ddl() const
{
  return is_inner_;
}

int ObGrantArg::assign(const ObGrantArg &other)
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
              is_inner_
              );
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

  //compatibility for old version
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

bool ObRevokeUserArg::is_valid() const
{
  // FIXME: Currently the role only supports revoke from user
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != user_id_;
}

OB_SERIALIZE_MEMBER((ObRevokeUserArg, ObDDLArg),
                    tenant_id_,
                    user_id_,
                    priv_set_,
                    revoke_all_,
                    role_ids_);

bool ObRevokeDBArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != user_id_
      && !db_.empty();
}

OB_SERIALIZE_MEMBER((ObRevokeDBArg, ObDDLArg),
                    tenant_id_,
                    user_id_,
                    db_,
                    priv_set_);

bool ObRevokeTableArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != user_id_
      && !db_.empty() && !table_.empty();
}

OB_SERIALIZE_MEMBER((ObRevokeTableArg, ObDDLArg),
                    tenant_id_,
                    user_id_,
                    db_,
                    table_,
                    priv_set_,
                    grant_,
                    obj_id_,
                    obj_type_,
                    grantor_id_,
                    obj_priv_array_,
                    revoke_all_ora_);

bool ObRevokeSysPrivArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != grantee_id_;
}

int ObRevokeSysPrivArg::assign(const ObRevokeSysPrivArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  grantee_id_ = other.grantee_id_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(sys_priv_array_.assign(other.sys_priv_array_))) {
    LOG_WARN("fail to assign sys_priv_array_", K(ret));
  } else if (OB_FAIL(role_ids_.assign(other.role_ids_))) {
    LOG_WARN("fail to assign role_ids_");
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObRevokeSysPrivArg, ObDDLArg),
                    tenant_id_,
                    grantee_id_,
                    sys_priv_array_,
                    role_ids_);

OB_SERIALIZE_MEMBER((ObCreateRoleArg, ObDDLArg),
                    tenant_id_,
                    user_infos_);

//----End of structs for managing privileges----

bool ObAdminSwitchReplicaRoleArg::is_valid() const
{
  return ls_id_ >= 0 || server_.is_valid() || !zone_.is_empty();
}

OB_SERIALIZE_MEMBER(ObAdminSwitchReplicaRoleArg,
    role_, ls_id_, server_, zone_, tenant_name_);

bool ObAdminSwitchRSRoleArg::is_valid() const
{
  return server_.is_valid() || !zone_.is_empty();
}

OB_SERIALIZE_MEMBER(ObAdminSwitchRSRoleArg,
    role_, server_, zone_);

OB_SERIALIZE_MEMBER(ObAdminDropReplicaArg,
    force_cmd_);

OB_SERIALIZE_MEMBER(ObAdminMigrateReplicaArg, force_cmd_);


bool ObPhysicalRestoreTenantArg::is_valid() const
{
  return !tenant_name_.empty()
         && tenant_name_.length() < common::OB_MAX_TENANT_NAME_LENGTH_STORE
         && (!uri_.empty() || !multi_uri_.empty())
         && uri_.length() < share::OB_MAX_RESTORE_DEST_LENGTH
         && !restore_option_.empty()
         && restore_option_.length() < common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH
         && (!with_restore_scn_ || restore_scn_.is_valid());
}

OB_SERIALIZE_MEMBER((ObPhysicalRestoreTenantArg, ObCmdArg),
                    tenant_name_,
                    uri_,
                    restore_option_,
                    restore_scn_,
                    kms_info_,
                    passwd_array_,
                    table_items_,
                    multi_uri_,
                    description_,
                    with_restore_scn_,
                    encrypt_key_,
                    kms_uri_,
                    kms_encrypt_key_,
                    restore_timestamp_,
                    initiator_job_id_,
                    initiator_tenant_id_);

ObPhysicalRestoreTenantArg::ObPhysicalRestoreTenantArg()
  : ObCmdArg(),
    tenant_name_(),
    uri_(),
    restore_option_(),
    restore_scn_(),
    kms_info_(),
    passwd_array_(),
    table_items_(),
    multi_uri_(),
    description_(),
    with_restore_scn_(false),
    encrypt_key_(),
    kms_uri_(),
    kms_encrypt_key_(),
    restore_timestamp_(),
    initiator_job_id_(0),
    initiator_tenant_id_(OB_INVALID_TENANT_ID)
{
}

int ObPhysicalRestoreTenantArg::assign(const ObPhysicalRestoreTenantArg &other)
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
    restore_scn_ = other.restore_scn_;
    kms_info_ = other.kms_info_;
    passwd_array_ = other.passwd_array_;
    description_ = other.description_;
    with_restore_scn_ = other.with_restore_scn_;
    encrypt_key_ = other.encrypt_key_;
    kms_uri_ = other.kms_uri_;
    kms_encrypt_key_ = other.kms_encrypt_key_;
    restore_timestamp_ = other.restore_timestamp_;
    initiator_job_id_ = other.initiator_job_id_;
    initiator_tenant_id_ = other.initiator_tenant_id_;
  }
  return ret;
}

int ObPhysicalRestoreTenantArg::add_table_item(const ObTableItem &item)
{
  return table_items_.push_back(item);
}

OB_SERIALIZE_MEMBER(ObServerZoneArg,
    server_, zone_);

OB_SERIALIZE_MEMBER(ObRefreshIOCalibrationArg,
                    storage_name_,
                    only_refresh_,
                    calibration_list_);

bool ObRefreshIOCalibrationArg::is_valid() const
{
  bool bret = !(only_refresh_ && calibration_list_.count() > 0);
  return bret;
}

int ObRefreshIOCalibrationArg::assign(const ObRefreshIOCalibrationArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calibration_list_.assign(other.calibration_list_))) {
    LOG_WARN("assign calibration list failed", K(ret));
  } else {
    storage_name_ = other.storage_name_;
    only_refresh_ = other.only_refresh_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObAdminRefreshIOCalibrationArg, ObServerZoneArg),
                    storage_name_,
                    only_refresh_,
                    calibration_list_);

bool ObAdminRefreshIOCalibrationArg::is_valid() const
{
  bool bret = ObServerZoneArg::is_valid()
    && !(only_refresh_ && calibration_list_.count() > 0);
  return bret;
}

int ObAdminRefreshIOCalibrationArg::assign(const ObAdminRefreshIOCalibrationArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calibration_list_.assign(other.calibration_list_))) {
    LOG_WARN("assign calibration list failed", K(ret));
  } else {
    server_ = other.server_;
    zone_ = other.zone_;
    storage_name_ = other.storage_name_;
    only_refresh_ = other.only_refresh_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObRunJobArg, ObServerZoneArg), job_);

ObUpgradeJobArg::ObUpgradeJobArg()
  : action_(INVALID_ACTION),
    version_(common::OB_INVALID_VERSION),
    tenant_ids_()
{
  tenant_ids_.set_label("UpgJobArr");
}
bool ObUpgradeJobArg::is_valid() const
{
  return INVALID_ACTION != action_
          && (UPGRADE_POST_ACTION != action_ || version_ > 0);
}
int ObUpgradeJobArg::assign(const ObUpgradeJobArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_ids_.assign(other.tenant_ids_))) {
    LOG_WARN("fail to assign tenant_ids", KR(ret));
  } else {
    action_ = other.action_;
    version_ = other.version_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObUpgradeJobArg, action_, version_, tenant_ids_);

int ObUpgradeTableSchemaArg::init(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const bool upgrade_virtual_schema)
{
  int ret = OB_SUCCESS;
  ObDDLArg::reset();
  exec_tenant_id_ = tenant_id;
  tenant_id_ = tenant_id;
  table_id_ = table_id;
  upgrade_virtual_schema_ = upgrade_virtual_schema;
  return ret;
}

bool ObUpgradeTableSchemaArg::is_valid() const
{
  return common::OB_INVALID_TENANT_ID != exec_tenant_id_
         && common::OB_INVALID_TENANT_ID != tenant_id_
         && (upgrade_virtual_schema_ || is_system_table(table_id_));
}

int ObUpgradeTableSchemaArg::assign(const ObUpgradeTableSchemaArg &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ObDDLArg", KR(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    table_id_ = other.table_id_;
    upgrade_virtual_schema_ = other.upgrade_virtual_schema_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObUpgradeTableSchemaArg, ObDDLArg), tenant_id_, table_id_, upgrade_virtual_schema_);

int ObAdminFlushCacheArg::assign(const ObAdminFlushCacheArg &other)
{
  int ret = OB_SUCCESS;
  cache_type_ = other.cache_type_;
  sql_id_ = other.sql_id_;
  is_fine_grained_ = other.is_fine_grained_;
  ns_type_ = other.ns_type_;
  if (OB_FAIL(tenant_ids_.assign(other.tenant_ids_))) {
    LOG_WARN("failed to assign tenant ids", K(ret));
  } else if (OB_FAIL(db_ids_.assign(other.db_ids_))) {
    LOG_WARN("failed to assign db ids", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAdminFlushCacheArg, tenant_ids_, cache_type_, db_ids_, sql_id_, is_fine_grained_, ns_type_);

int ObFlushCacheArg::assign(const ObFlushCacheArg &other)
{
  int ret = OB_SUCCESS;
  is_all_tenant_ = other.is_all_tenant_;
  tenant_id_ = other.tenant_id_;
  cache_type_ = other.cache_type_;
  sql_id_ = other.sql_id_;
  is_fine_grained_ = other.is_fine_grained_;
  ns_type_ = other.ns_type_;
  if (OB_FAIL(db_ids_.assign(other.db_ids_))) {
    LOG_WARN("failed to assign db ids", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObFlushCacheArg, is_all_tenant_, tenant_id_, cache_type_, db_ids_, sql_id_, is_fine_grained_, ns_type_);

OB_SERIALIZE_MEMBER(ObGetAllSchemaArg,
                    schema_version_,
                    tenant_name_);

bool ObAdminMergeArg::is_valid() const
{
  // empty zone means all zone
  return type_ >= START_MERGE && type_ <= RESUME_MERGE;
}

int ObAdminMergeArg::assign(const ObAdminMergeArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    type_ = other.type_;
    affect_all_ = other.affect_all_;
    affect_all_user_ = other.affect_all_user_;
    affect_all_meta_ = other.affect_all_meta_;
    if (OB_FAIL(tenant_ids_.assign(other.tenant_ids_))) {
      LOG_WARN("fail to assign tenant_ids", KR(ret), K(other));
    }
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObAdminMergeArg,
   type_, affect_all_, tenant_ids_, affect_all_user_, affect_all_meta_);

bool ObAdminRecoveryArg::is_valid() const
{
  return type_ >= SUSPEND_RECOVERY && type_ <= RESUME_RECOVERY;
}

OB_SERIALIZE_MEMBER(ObAdminRecoveryArg, type_, zone_);

OB_SERIALIZE_MEMBER(ObAdminClearRoottableArg,
   tenant_name_);

OB_SERIALIZE_MEMBER(ObAdminSetConfigItem,
    name_, value_, comment_, zone_, server_, tenant_name_, exec_tenant_id_, tenant_ids_,
    want_to_set_tenant_config_);

OB_SERIALIZE_MEMBER(ObAdminSetConfigArg, items_, is_inner_, is_backup_config_);

int ObAdminSetConfigArg::assign(const ObAdminSetConfigArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(items_.assign(arg.items_))) {
    LOG_WARN("fail to assign items", K(ret));
  } else {
    is_inner_ = arg.is_inner_;
    is_backup_config_ = arg.is_backup_config_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAdminServerArg,
                    servers_,
                    zone_,
                    force_stop_,
                    op_);

OB_SERIALIZE_MEMBER(ObAdminZoneArg,
                    zone_,
                    region_,
                    idc_,
                    alter_zone_options_,
                    zone_type_,
                    sql_stmt_str_,
                    force_stop_,
                    op_);

OB_SERIALIZE_MEMBER(ObAutoincSyncArg,
                    tenant_id_, table_id_, column_id_, table_part_num_, auto_increment_, sync_value_);

OB_SERIALIZE_MEMBER(ObAdminChangeReplicaArg, force_cmd_);

#ifdef OB_BUILD_ARBITRATION
int ObAdminAddArbitrationServiceArg::init(const ObString &arbitration_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arbitration_service.length() > OB_MAX_ARBITRATION_SERVICE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "arbitration service, length oversize");
  } else if (OB_FAIL(arbitration_service_.assign(arbitration_service))) {
    LOG_WARN("fali to assign arbitration service", KR(ret), K(arbitration_service));
  }
  return ret;
}

int ObAdminRemoveArbitrationServiceArg::init(const ObString &arbitration_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arbitration_service.length() > OB_MAX_ARBITRATION_SERVICE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "arbitration service, length oversize");
  } else if (OB_FAIL(arbitration_service_.assign(arbitration_service))) {
    LOG_WARN("fali to assign arbitration service", KR(ret), K(arbitration_service));
  }
  return ret;
}

int ObAdminReplaceArbitrationServiceArg::init(
    const ObString &arbitration_service,
    const ObString &previous_arbitration_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arbitration_service.length() > OB_MAX_ARBITRATION_SERVICE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "current arbitration service, length oversize");
  } else if (OB_UNLIKELY(previous_arbitration_service.length() > OB_MAX_ARBITRATION_SERVICE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(previous_arbitration_service));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "new arbitration service, length oversize");
  } else if (OB_FAIL(arbitration_service_.assign(arbitration_service))) {
    LOG_WARN("fail to assign arbitration_service", KR(ret), K(arbitration_service));
  } else if (OB_FAIL(previous_arbitration_service_.assign(previous_arbitration_service))) {
    LOG_WARN("fail to assign previous arbitration service", KR(ret), K(previous_arbitration_service));
  }
  return ret;
}

int ObAdminReplaceArbitrationServiceArg::assign(const ObAdminReplaceArbitrationServiceArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(arbitration_service_.assign(other.arbitration_service_))) {
    LOG_WARN("fail to assign arbitration service", KR(ret), K(other));
  } else if (OB_FAIL(previous_arbitration_service_.assign(other.previous_arbitration_service_))) {
    LOG_WARN("fail to assign previous arbitration service", KR(ret), K(other));
  }
  return ret;
}

int ObRemoveClusterInfoFromArbServerArg::init(
    const ObString &arbitration_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arbitration_service.length() > OB_MAX_ARBITRATION_SERVICE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service));
  } else if (OB_FAIL(arbitration_service_.assign(arbitration_service))) {
    LOG_WARN("fail to assign arbitration_service", KR(ret), K(arbitration_service));
  }
  return ret;
}

int ObRemoveClusterInfoFromArbServerArg::assign(const ObRemoveClusterInfoFromArbServerArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(arbitration_service_.assign(other.arbitration_service_))) {
    LOG_WARN("fail to assign arbitration service", KR(ret), K(other));
  }
  return ret;
}
#endif

bool ObUpdateIndexStatusArg::is_allow_when_disable_ddl() const
{
  bool bret = false;
  if (is_error_index_status(status_)) {
    bret = true;
  }
  return bret;
}

bool ObUpdateIndexStatusArg::is_valid() const
{
  return OB_INVALID_ID != index_table_id_ && status_ > INDEX_STATUS_NOT_FOUND
      && status_ < INDEX_STATUS_MAX;
}

OB_SERIALIZE_MEMBER((ObUpdateIndexStatusArg, ObDDLArg),
                    index_table_id_,
                    status_,
                    convert_status_,
                    in_offline_ddl_white_list_);

OB_SERIALIZE_MEMBER(ObMergeFinishArg, server_, frozen_version_);

OB_SERIALIZE_MEMBER(ObDebugSyncActionArg, reset_, clear_, action_);


OB_SERIALIZE_MEMBER(ObAdminMigrateUnitArg, unit_id_, is_cancel_ ,destination_);

OB_SERIALIZE_MEMBER(ObCheckpointSlogArg, tenant_id_);

OB_SERIALIZE_MEMBER(ObDumpMemtableArg, tenant_id_, ls_id_ ,tablet_id_);

OB_SERIALIZE_MEMBER(ObDumpTxDataMemtableArg, tenant_id_, ls_id_);

OB_SERIALIZE_MEMBER(ObDumpSingleTxDataArg, tenant_id_, ls_id_, tx_id_);

#ifdef OB_BUILD_ARBITRATION
OB_SERIALIZE_MEMBER(ObAdminAddArbitrationServiceArg, arbitration_service_);

OB_SERIALIZE_MEMBER(ObAdminRemoveArbitrationServiceArg, arbitration_service_);

OB_SERIALIZE_MEMBER(ObAdminReplaceArbitrationServiceArg, arbitration_service_, previous_arbitration_service_);

OB_SERIALIZE_MEMBER(ObRemoveClusterInfoFromArbServerArg, arbitration_service_);
#endif

int ObRootMajorFreezeArg::assign(const ObRootMajorFreezeArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(try_frozen_versions_.assign(other.try_frozen_versions_))) {
      LOG_WARN("fail to assign try_frozen_versions", K(ret), K(other.try_frozen_versions_));
    } else if (OB_FAIL(tenant_ids_.assign(other.tenant_ids_))) {
      LOG_WARN("fail to assign tenant_ids", K(ret), K(other.tenant_ids_));
    } else {
      svr_ = other.svr_;
      freeze_all_ = other.freeze_all_;
    }
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObRootMajorFreezeArg,
                    try_frozen_versions_,
                    svr_,
                    tenant_ids_,
                    freeze_all_);

OB_SERIALIZE_MEMBER(ObMinorFreezeArg,
                    tenant_ids_,
                    tablet_id_,
                    ls_id_);

int ObMinorFreezeArg::assign(const ObMinorFreezeArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_ids_.assign(other.tenant_ids_))) {
    LOG_WARN("assign tenant_ids_ failed", K(ret), K(other.tenant_ids_));
  } else {
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRootMinorFreezeArg,
                    tenant_ids_,
                    server_list_,
                    zone_,
                    tablet_id_,
                    ls_id_);

int ObRootMinorFreezeArg::assign(const ObRootMinorFreezeArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_ids_.assign(other.tenant_ids_))) {
    LOG_WARN("assign tenant_ids_ failed", K(ret), K(other.tenant_ids_));
  } else if (OB_FAIL(server_list_.assign(other.server_list_))) {
    LOG_WARN("assign server_list_ failed", K(ret), K(other.server_list_));
  } else if (OB_FAIL(zone_.assign(other.zone_))) {
    LOG_WARN("assign zone_ failed", K(ret), K(other.zone_));
  } else {
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSyncPGPartitionMTFinishArg, server_, version_);

OB_SERIALIZE_MEMBER(ObCheckDanglingReplicaFinishArg, server_, version_, dangling_count_);

OB_SERIALIZE_MEMBER(ObMemberListAndLeaderArg,
                    member_list_,
                    leader_,
                    self_,
                    lower_list_,
                    replica_type_,
                    property_,
                    role_);

bool ObMemberListAndLeaderArg::is_valid() const
{
  return member_list_.count() > 0
         && self_.is_valid()
         && common::REPLICA_TYPE_MAX != replica_type_
         && property_.is_valid()
         && (common::INVALID_ROLE <= role_ && role_ <= common::STANDBY_LEADER);
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

int ObMemberListAndLeaderArg::assign(const ObMemberListAndLeaderArg &other)
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

OB_SERIALIZE_MEMBER(ObGetMemberListAndLeaderResult,
                    member_list_,
                    leader_,
                    self_,
                    lower_list_,
                    replica_type_,
                    property_);

void ObGetMemberListAndLeaderResult::reset()
{
  member_list_.reset();
  leader_.reset();
  self_.reset();
  lower_list_.reset();
  replica_type_ = common::REPLICA_TYPE_MAX;
}

int ObGetMemberListAndLeaderResult::assign(const ObGetMemberListAndLeaderResult &other)
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
  return OB_INVALID_ID != outline_info_.get_tenant_id()
      && !outline_info_.get_name_str().empty()
      && !(outline_info_.get_signature_str().empty() && !ObOutlineInfo::is_sql_id_valid(outline_info_.get_sql_id_str()))
      && (!outline_info_.get_outline_content_str().empty() || outline_info_.has_outline_params())
      && !(outline_info_.get_sql_text_str().empty() && !ObOutlineInfo::is_sql_id_valid(outline_info_.get_sql_id_str()));
}

OB_SERIALIZE_MEMBER((ObCreateOutlineArg, ObDDLArg),
                    or_replace_,
                    outline_info_,
                    db_name_);

OB_SERIALIZE_MEMBER((ObCreateUserDefinedFunctionArg, ObDDLArg),
                     udf_);

OB_SERIALIZE_MEMBER((ObDropUserDefinedFunctionArg, ObDDLArg),
                     tenant_id_,
                     name_,
                     if_exist_);

OB_SERIALIZE_MEMBER((ObAlterOutlineArg, ObDDLArg),
                    alter_outline_info_,
                    db_name_);

bool ObDropOutlineArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !db_name_.empty() && !outline_name_.empty();
}

OB_SERIALIZE_MEMBER((ObDropOutlineArg, ObDDLArg),
                    tenant_id_,
                    db_name_,
                    outline_name_);

bool ObCreateDbLinkArg::is_valid() const
{
  return OB_INVALID_ID != dblink_info_.get_tenant_id()
          && OB_INVALID_ID != dblink_info_.get_owner_id()
          && OB_INVALID_ID != dblink_info_.get_dblink_id()
          && !dblink_info_.get_dblink_name().empty()
          && !dblink_info_.get_tenant_name().empty()
          && !dblink_info_.get_user_name().empty()
          && dblink_info_.get_host_addr().is_valid()
          && (!dblink_info_.get_password().empty() || !dblink_info_.get_encrypted_password().empty());

}

OB_SERIALIZE_MEMBER((ObCreateDbLinkArg, ObDDLArg), dblink_info_);

bool ObDropDbLinkArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !dblink_name_.empty();
}

OB_SERIALIZE_MEMBER((ObDropDbLinkArg, ObDDLArg), tenant_id_, dblink_name_, if_exist_);

OB_SERIALIZE_MEMBER((ObUseDatabaseArg, ObDDLArg));
OB_SERIALIZE_MEMBER(ObGetPartitionCountResult, partition_count_);

OB_SERIALIZE_MEMBER(ObFetchAliveServerArg, cluster_id_);

OB_SERIALIZE_MEMBER(ObFetchAliveServerResult, active_server_list_, inactive_server_list_);
OB_SERIALIZE_MEMBER(ObFetchActiveServerAddrResult, server_addr_list_);

int ObFetchActiveServerAddrResult::assign(const ObFetchActiveServerAddrResult &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_addr_list_.assign(other.server_addr_list_))) {
    LOG_WARN("failed to assign server status list", KR(ret), K(other));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAdminSetTPArg,
                    event_no_,
                    event_name_,
                    occur_,
                    trigger_freq_,
                    error_code_,
                    server_,
                    zone_,
                    cond_);

bool ObCreateRoutineArg::is_valid() const
{
  return OB_INVALID_ID != routine_info_.get_tenant_id()
      && !routine_info_.get_routine_name().empty()
      && routine_info_.get_routine_type() != INVALID_ROUTINE_TYPE
      && !routine_info_.get_routine_body().empty();
}

int ObCreateRoutineArg::assign(const ObCreateRoutineArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(routine_info_.assign(other.routine_info_))) {
    LOG_WARN("failed to copy routine info", K(ret));
  } else if (OB_FAIL(error_info_.assign(other.error_info_))) {
    LOG_WARN("failed to copy error info", K(ret));
  } else if (OB_FAIL(dependency_infos_.assign(other.dependency_infos_))) {
    LOG_WARN("failed to copy dependency info", K(ret));
  } else {
    db_name_ = other.db_name_;
    is_or_replace_ = other.is_or_replace_;
    is_need_alter_ = other.is_need_alter_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateRoutineArg, ObDDLArg),
                    routine_info_, db_name_,
                    is_or_replace_, is_need_alter_,
                    error_info_, dependency_infos_);

bool ObDropRoutineArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !routine_name_.empty() && routine_type_ != INVALID_ROUTINE_TYPE;
}

OB_SERIALIZE_MEMBER((ObDropRoutineArg, ObDDLArg),
                    tenant_id_, db_name_,
                    routine_name_, routine_type_,
                    if_exist_, error_info_);

bool ObCreatePackageArg::is_valid() const
{
  return !db_name_.empty() && package_info_.is_valid();
}

int ObCreatePackageArg::assign(const ObCreatePackageArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(package_info_.assign(other.package_info_))) {
    LOG_WARN("failed to copy package info", K(ret));
  } else if (OB_FAIL(public_routine_infos_.assign(other.public_routine_infos_))) {
    LOG_WARN("failed to copy routine info", K(ret));
  } else if (OB_FAIL(error_info_.assign(other.error_info_))) {
    LOG_WARN("failed to copy error info", K(ret));
  } else if (OB_FAIL(dependency_infos_.assign(other.dependency_infos_))) {
    LOG_WARN("failed to copy dependency info", K(ret));
  } else {
    db_name_ = other.db_name_;
    is_replace_ = other.is_replace_;
    is_editionable_ = other.is_editionable_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreatePackageArg, ObDDLArg), is_replace_,
                    is_editionable_, db_name_, package_info_,
                    public_routine_infos_, error_info_, dependency_infos_);

bool ObAlterPackageArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && !db_name_.empty()
      && !package_name_.empty()
      && INVALID_PACKAGE_TYPE != package_type_;
}

int ObAlterPackageArg::assign(const ObAlterPackageArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(public_routine_infos_.assign(other.public_routine_infos_))) {
    LOG_WARN("fail to assign array", K(ret));
  } else if (OB_FAIL(error_info_.assign(other.error_info_))) {
    LOG_WARN("failed to copy error info", K(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    db_name_ = other.db_name_;
    package_name_ = other.package_name_;
    package_type_ = other.package_type_;
    compatible_mode_ = other.compatible_mode_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObAlterPackageArg, ObDDLArg), tenant_id_, db_name_, package_name_, package_type_,
                    compatible_mode_, public_routine_infos_, error_info_);

bool ObDropPackageArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && !db_name_.empty()
      && !package_name_.empty();
}

OB_SERIALIZE_MEMBER((ObDropPackageArg, ObDDLArg), tenant_id_,
                    db_name_, package_name_, package_type_,
                    compatible_mode_, error_info_);

bool ObCreateTriggerArg::is_valid() const
{
  return !trigger_database_.empty()
      && !base_object_name_.empty()
      && trigger_info_.is_valid_for_create();
}

int ObCreateTriggerArg::assign(const ObCreateTriggerArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(trigger_info_.assign(other.trigger_info_))) {
    LOG_WARN("failed to copy trigger info", K(ret));
  } else if (OB_FAIL(error_info_.assign(other.error_info_))) {
    LOG_WARN("failed to copy error info", K(ret));
  } else if (OB_FAIL(dependency_infos_.assign(other.dependency_infos_))) {
    LOG_WARN("failed to copy dependency info", K(ret));
  } else {
    trigger_database_ = other.trigger_database_;
    base_object_database_ = other.base_object_database_;
    base_object_name_ = other.base_object_name_;
    flags_ = other.flags_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateTriggerArg, ObDDLArg),
                    trigger_database_,
                    base_object_database_,
                    base_object_name_,
                    trigger_info_,
                    flags_,
                    error_info_,
                    dependency_infos_);

bool ObDropTriggerArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && !trigger_database_.empty()
      && !trigger_name_.empty();
}

OB_SERIALIZE_MEMBER((ObDropTriggerArg, ObDDLArg),
                    tenant_id_,
                    trigger_database_,
                    trigger_name_,
                    if_exist_);

bool ObAlterTriggerArg::is_valid() const
{
  return trigger_infos_.count() != 0;
}

int ObAlterTriggerArg::assign(const ObAlterTriggerArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(trigger_info_.assign(other.trigger_info_))) {
    LOG_WARN("failed to copy trigger info", K(ret));
  } else if (OB_FAIL(trigger_infos_.assign(other.trigger_infos_))) {
    LOG_WARN("failed to copy trigger infos", K(ret));
  } else {
    trigger_database_ = other.trigger_database_;
    is_set_status_ = other.is_set_status_;
    is_alter_compile_ = other.is_alter_compile_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObAlterTriggerArg, ObDDLArg), trigger_database_,
                    trigger_info_, trigger_infos_, is_set_status_, is_alter_compile_);

bool ObCreateUDTArg::is_valid() const
{
  return OB_INVALID_ID != udt_info_.get_tenant_id()
      && !db_name_.empty()
      && !udt_info_.get_type_name().empty()
      && udt_info_.get_typecode() != UDT_INVALID_TYPE_CODE;
}

int ObCreateUDTArg::assign(const ObCreateUDTArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(udt_info_.assign(other.udt_info_))) {
    LOG_WARN("failed to copy udt info", K(ret));
  } else if (OB_FAIL(public_routine_infos_.assign(other.public_routine_infos_))) {
    LOG_WARN("failed to copy public routine info", K(ret));
  } else if (OB_FAIL(error_info_.assign(other.error_info_))) {
    LOG_WARN("failed to copy error info", K(ret));
  } else if (OB_FAIL(dependency_infos_.assign(other.dependency_infos_))) {
    LOG_WARN("failed to copy depend info", K(ret));
  } else {
    db_name_ = other.db_name_;
    is_or_replace_ = other.is_or_replace_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateUDTArg, ObDDLArg), udt_info_, db_name_, is_or_replace_,
                                                error_info_,
                                                public_routine_infos_,
                                                dependency_infos_);

bool ObDropUDTArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !db_name_.empty() && !udt_name_.empty();
}

OB_SERIALIZE_MEMBER((ObDropUDTArg, ObDDLArg), tenant_id_, db_name_,
                     udt_name_, if_exist_, is_type_body_, force_or_validate_);

OB_SERIALIZE_MEMBER(ObCancelTaskArg, task_id_);

int ObReportSingleReplicaArg::assign(const ObReportSingleReplicaArg &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ls_id_ = other.ls_id_;
    tenant_id_ = other.tenant_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObReportSingleReplicaArg, tenant_id_, ls_id_);
OB_SERIALIZE_MEMBER(ObSetDiskValidArg);

int ObCreateSynonymArg::assign(const ObCreateSynonymArg &other)
{
  int ret = OB_SUCCESS;
  db_name_ = other.db_name_;
  obj_db_name_ = other.obj_db_name_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("failed to assign ddl arg", K(ret));
  } else if (OB_FAIL(synonym_info_.assign(other.synonym_info_))) {
    LOG_WARN("failed to assign synonym info", K(ret));
  } else if (OB_FAIL(dependency_info_.assign(other.dependency_info_))) {
    LOG_WARN("failed to assign dependency info", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateSynonymArg, ObDDLArg),
                    or_replace_,
                    synonym_info_,
                    db_name_,
                    obj_db_name_,
                    dependency_info_);

OB_SERIALIZE_MEMBER((ObDropSynonymArg, ObDDLArg),
                    tenant_id_,
                    is_force_,
                    db_name_,
                    synonym_name_);

bool ObDropSynonymArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && !db_name_.empty() && !synonym_name_.empty();
}

OB_SERIALIZE_MEMBER(ObAdminClearDRTaskArg, tenant_ids_, type_, zone_names_);
int ObAdminClearDRTaskArg::assign(
    const ObAdminClearDRTaskArg &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_ids_.assign(that.tenant_ids_))) {
    LOG_WARN("fail to assign tenant_ids", KR(ret));
  } else {
    type_ = that.type_;
    if (OB_FAIL(zone_names_.assign(that.zone_names_))) {
      LOG_WARN("fail to assign zone_names", KR(ret));
    }
  }
  return ret;
}


OB_SERIALIZE_MEMBER(ObAdminClearBalanceTaskArg, tenant_ids_, type_, zone_names_);

OB_SERIALIZE_MEMBER(ObMCLogInfo, log_id_, timestamp_);

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
  return OB_INVALID_TENANT_ID != tenant_id_
         && OB_INVALID_ID != last_replay_log_id_
         && share::is_tenant_table(table_id_);
}

DEF_TO_STRING(ObForceCreateSysTableArg)
{
  int64_t pos = 0;
  J_KV(K(tenant_id_),
       K(table_id_),
       K(last_replay_log_id_));
  return pos;
}

OB_SERIALIZE_MEMBER(ObForceCreateSysTableArg, tenant_id_, table_id_, last_replay_log_id_);

bool ObForceSetLocalityArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != exec_tenant_id_ && locality_.length() > 0;
}
int ObForceSetLocalityArg::assign(const ObForceSetLocalityArg &other)
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

OB_SERIALIZE_MEMBER(ObSplitPartitionArg, split_info_);
OB_SERIALIZE_MEMBER(ObSplitPartitionResult, results_);

DEF_TO_STRING(ObUpdateStatCacheArg)
{
  int64_t pos = 0;
  J_KV(K_(table_id),
       K_(tenant_id),
       K_(partition_ids),
       K_(column_ids),
       K_(no_invalidate));
  return pos;
}
OB_SERIALIZE_MEMBER(ObUpdateStatCacheArg, tenant_id_, table_id_, partition_ids_, column_ids_, no_invalidate_);
OB_SERIALIZE_MEMBER(ObSplitPartitionBatchArg, split_info_);
OB_SERIALIZE_MEMBER((ObSequenceDDLArg, ObDDLArg),
                    stmt_type_,
                    option_bitset_,
                    seq_schema_,
                    database_name_,
                    ignore_exists_error_);

OB_SERIALIZE_MEMBER((ObTablespaceDDLArg, ObDDLArg), schema_, type_);
DEF_TO_STRING(ObTablespaceDDLArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(schema),
       K_(type));
  J_OBJ_END();
  return pos;
}
bool ObTablespaceDDLArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != schema_.get_tenant_id()
      && (CREATE_TABLESPACE == type_ || DROP_TABLESPACE == type_ || ALTER_TABLESPACE == type_);
}

OB_SERIALIZE_MEMBER(ObBootstrapArg, server_list_, cluster_role_);

int ObBootstrapArg::assign(const ObBootstrapArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_list_.assign(arg.server_list_))) {
    LOG_WARN("fail to assign", KR(ret), K(arg));
  } else {
    cluster_role_ = arg.cluster_role_;
  }
  return ret;
}

bool ObSplitPartitionBatchArg::is_valid() const
{
  return split_info_.is_valid();
}

int ObSplitPartitionBatchArg::assign(const ObSplitPartitionBatchArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(split_info_, other.split_info_))) {
    SHARE_LOG(WARN, "failed to assign args_", K(ret));
  }
  return ret;
}

DEF_TO_STRING(ObSplitPartitionBatchArg)
{
  int64_t pos = 0;
  J_KV(K_(split_info));
  return pos;
}

bool ObCheckpoint::is_valid() const
{
  return (ls_id_.is_valid() && cur_sync_scn_.is_valid_and_not_min() && cur_restore_source_max_scn_.is_valid_and_not_min());
}

bool ObCheckpoint::operator==(const obrpc::ObCheckpoint &r) const
{
  return ls_id_ == r.ls_id_ && cur_sync_scn_ == r.cur_sync_scn_ && cur_restore_source_max_scn_ == r.cur_restore_source_max_scn_;
}

OB_SERIALIZE_MEMBER(ObCheckpoint, ls_id_, cur_sync_scn_, cur_restore_source_max_scn_);

OB_SERIALIZE_MEMBER(ObGetWRSArg, tenant_id_, scope_, need_filter_);
OB_SERIALIZE_MEMBER(ObGetWRSResult, self_addr_, err_code_);
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
  OB_UNIS_ADD_LEN(tablet_id_);
  OB_UNIS_ADD_LEN(ls_id_);
  OB_UNIS_ADD_LEN(tenant_id_);
  OB_UNIS_ADD_LEN(tx_id_);

  return len;
}

int ObEstPartArgElement::serialize(char *buf,
                                   const int64_t buf_len,
                                   int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(scan_flag_);
  OB_UNIS_ENCODE(index_id_);
  OB_UNIS_ENCODE(range_columns_count_);
  OB_UNIS_ENCODE(batch_);
  OB_UNIS_ENCODE(tablet_id_);
  OB_UNIS_ENCODE(ls_id_);
  OB_UNIS_ENCODE(tenant_id_);
  OB_UNIS_ENCODE(tx_id_);

  return ret;
}

int ObEstPartArgElement::deserialize(common::ObIAllocator &allocator,
                                     const char *buf,
                                     const int64_t data_len,
                                     int64_t &pos)
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
  OB_UNIS_DECODE(tablet_id_);
  OB_UNIS_DECODE(ls_id_);
  OB_UNIS_DECODE(tenant_id_);
  OB_UNIS_DECODE(tx_id_);
  return ret;
}

void ObEstPartArg::reset()
{
  for (int64_t i = 0; i < index_params_.count(); ++i) {
    index_params_.at(i).batch_.destroy();
  }
}

OB_DEF_SERIALIZE_SIZE(ObEstPartArg)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(schema_version_);
  OB_UNIS_ADD_LEN(index_params_);
  return len;
}

OB_DEF_SERIALIZE(ObEstPartArg)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(schema_version_);
  OB_UNIS_ENCODE(index_params_);
  return ret;
}

OB_DEF_DESERIALIZE(ObEstPartArg)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(schema_version_);
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
  return ret;
}

OB_SERIALIZE_MEMBER(ObEstPartResElement, logical_row_count_,
                                         physical_row_count_,
                                         reliable_,
                                         est_records_);

OB_SERIALIZE_MEMBER(ObEstPartRes, index_param_res_);

OB_SERIALIZE_MEMBER(TenantServerUnitConfig,
                    tenant_id_,
                    compat_mode_,
                    unit_config_,
                    replica_type_,
                    is_delete_,
                    if_not_grant_,
                    unit_id_);

int ObForceSetLSAsSingleReplicaArg::init(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
  }
  return ret;
}

bool ObForceSetLSAsSingleReplicaArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid();
}

int ObForceSetLSAsSingleReplicaArg::assign(const ObForceSetLSAsSingleReplicaArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObForceSetLSAsSingleReplicaArg, tenant_id_, ls_id_);

OB_SERIALIZE_MEMBER(ObGetLSSyncScnArg, tenant_id_, ls_id_, check_sync_to_latest_);

bool ObGetLSSyncScnArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid();
}
int ObGetLSSyncScnArg::init(
    const uint64_t tenant_id, const ObLSID &ls_id, const bool check_sync_to_latest)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    check_sync_to_latest_ = check_sync_to_latest;
  }
  return ret;
}
int ObGetLSSyncScnArg::assign(const ObGetLSSyncScnArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    check_sync_to_latest_ = other.check_sync_to_latest_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObGetLSSyncScnRes, tenant_id_, ls_id_, cur_sync_scn_, cur_restore_source_max_scn_);

bool ObGetLSSyncScnRes::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid()
         && cur_sync_scn_.is_valid_and_not_min()
         && cur_restore_source_max_scn_.is_valid_and_not_min();
}
int ObGetLSSyncScnRes::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &cur_sync_scn,
    const share::SCN &cur_restore_source_max_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid()
                  || !cur_sync_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(cur_sync_scn), K(cur_restore_source_max_scn));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    cur_sync_scn_ = cur_sync_scn;
    cur_restore_source_max_scn_ = cur_restore_source_max_scn;
  }
  return ret;
}

int ObGetLSSyncScnRes::assign(const ObGetLSSyncScnRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    cur_sync_scn_ = other.cur_sync_scn_;
    cur_restore_source_max_scn_ = other.cur_restore_source_max_scn_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObGetLSReplayedScnArg, tenant_id_, ls_id_);

bool ObGetLSReplayedScnArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid();
}
int ObGetLSReplayedScnArg::init(
    const uint64_t tenant_id, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
  }
  return ret;
}
int ObGetLSReplayedScnArg::assign(const ObGetLSReplayedScnArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObGetLSReplayedScnRes, tenant_id_, ls_id_, cur_readable_scn_);

bool ObGetLSReplayedScnRes::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid()
         && cur_readable_scn_.is_valid_and_not_min();
}
int ObGetLSReplayedScnRes::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &cur_readable_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid()
                  || !cur_readable_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(cur_readable_scn));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    cur_readable_scn_ = cur_readable_scn;
  }
  return ret;
}

int ObGetLSReplayedScnRes::assign(const ObGetLSReplayedScnRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    cur_readable_scn_ = other.cur_readable_scn_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRefreshTenantInfoArg, tenant_id_);

bool ObRefreshTenantInfoArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}
int ObRefreshTenantInfoArg::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}
int ObRefreshTenantInfoArg::assign(const ObRefreshTenantInfoArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRefreshTenantInfoRes, tenant_id_);

bool ObRefreshTenantInfoRes::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}

int ObRefreshTenantInfoRes::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

int ObRefreshTenantInfoRes::assign(const ObRefreshTenantInfoRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObUpdateTenantInfoCacheArg, tenant_id_, tenant_info_, ora_rowscn_);

bool ObUpdateTenantInfoCacheArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && tenant_info_.is_valid()
         && 0 != ora_rowscn_;
}

int ObUpdateTenantInfoCacheArg::init(
    const uint64_t tenant_id,
    const ObAllTenantInfo &tenant_info,
    const int64_t ora_rowscn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !tenant_info.is_valid()
                  || 0 == ora_rowscn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_info), K(ora_rowscn));
  } else {
    tenant_id_ = tenant_id;
    tenant_info_ = tenant_info;
    ora_rowscn_ = ora_rowscn;
  }
  return ret;
}

int ObUpdateTenantInfoCacheArg::assign(const ObUpdateTenantInfoCacheArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    tenant_info_ = other.tenant_info_;
    ora_rowscn_ = other.ora_rowscn_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObUpdateTenantInfoCacheRes, tenant_id_);

bool ObUpdateTenantInfoCacheRes::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}

int ObUpdateTenantInfoCacheRes::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

int ObUpdateTenantInfoCacheRes::assign(const ObUpdateTenantInfoCacheRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
  }
  return ret;
}

int ObSwitchTenantArg::init(
    const uint64_t exec_tenant_id,
    const OpType op_type,
    const ObString &tenant_name,
    const bool is_verify)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id
                  || OpType::INVALID == op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exec_tenant_id), K(op_type), K(tenant_name));
  } else {
    exec_tenant_id_ = exec_tenant_id;
    op_type_ = op_type;
    tenant_name_ = tenant_name;
    is_verify_ = is_verify;
  }
  return ret;
}

int ObSwitchTenantArg::assign(const ObSwitchTenantArg &other)
{
  int ret = OB_SUCCESS;

  exec_tenant_id_ = other.exec_tenant_id_;
  op_type_ = other.op_type_;
  tenant_name_ = other.tenant_name_;
  stmt_str_ = other.stmt_str_;
  is_verify_ = other.is_verify_;

  return ret;
}

OB_SERIALIZE_MEMBER(ObSwitchTenantArg, exec_tenant_id_, op_type_, tenant_name_, stmt_str_, is_verify_);

int ObRecoverTenantArg::init(
    const uint64_t exec_tenant_id,
    const ObString &tenant_name,
    const RecoverType type,
    const SCN &recovery_until_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id || !is_valid(type, recovery_until_scn))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exec_tenant_id), K(type), K(recovery_until_scn));
  } else {
    exec_tenant_id_ = exec_tenant_id;
    tenant_name_ = tenant_name;
    type_ = type;
    recovery_until_scn_ = recovery_until_scn;
  }
  return ret;
}

int ObRecoverTenantArg::assign(const ObRecoverTenantArg &other)
{
  int ret = OB_SUCCESS;

  exec_tenant_id_ = other.exec_tenant_id_;
  tenant_name_ = other.tenant_name_;
  type_ = other.type_;
  recovery_until_scn_ = other.recovery_until_scn_;
  stmt_str_ = other.stmt_str_;

  return ret;
}


OB_SERIALIZE_MEMBER(ObRecoverTenantArg, exec_tenant_id_, tenant_name_, type_, recovery_until_scn_,
                                        stmt_str_);

OB_SERIALIZE_MEMBER((ObDDLNopOpreatorArg, ObDDLArg),
                     schema_operation_);
OB_SERIALIZE_MEMBER(ObTenantSchemaVersions, tenant_schema_versions_);

bool TenantServerUnitConfig::is_valid() const
{
  return common::OB_INVALID_ID != tenant_id_
         && ((lib::Worker::CompatMode::INVALID != compat_mode_
               && unit_config_.is_valid()
               && replica_type_ != common::ObReplicaType::REPLICA_TYPE_MAX)
             || (is_delete_));
}

int TenantServerUnitConfig::init(
    const uint64_t tenant_id,
    const uint64_t unit_id,
    const lib::Worker::CompatMode compat_mode,
    const share::ObUnitConfig &unit_config,
    const common::ObReplicaType replica_type,
    const bool if_not_grant,
    const bool is_delete)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
    // do not check others validation, since their validations vary
  } else {
    tenant_id_ = tenant_id;
    unit_id_ = unit_id;
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


OB_SERIALIZE_MEMBER((ObGetSchemaArg, ObDDLArg), reserve_, ignore_fail_);
OB_SERIALIZE_MEMBER(ObBroadcastSchemaArg, tenant_id_, schema_version_);

void ObBroadcastSchemaArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  schema_version_ = OB_INVALID_VERSION;
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
int ObGetRecycleSchemaVersionsArg::assign(const ObGetRecycleSchemaVersionsArg &other)
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
int ObGetRecycleSchemaVersionsResult::assign(const ObGetRecycleSchemaVersionsResult &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(recycle_schema_versions_.assign(other.recycle_schema_versions_))) {
      LOG_WARN("fail to assign recycle_schema_versions", KR(ret), K(other));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObHaGtsPingRequest, gts_id_, req_id_, epoch_id_, request_ts_);
OB_SERIALIZE_MEMBER(ObHaGtsPingResponse, gts_id_, req_id_, epoch_id_, response_ts_);
OB_SERIALIZE_MEMBER(ObHaGtsGetRequest, gts_id_, self_addr_, tenant_id_, srr_.mts_);
OB_SERIALIZE_MEMBER(ObHaGtsGetResponse, gts_id_, tenant_id_, srr_.mts_, gts_);
OB_SERIALIZE_MEMBER(ObHaGtsHeartbeat, gts_id_, addr_);
OB_SERIALIZE_MEMBER(ObHaGtsUpdateMetaRequest, gts_id_, epoch_id_, member_list_, local_ts_);
OB_SERIALIZE_MEMBER(ObHaGtsUpdateMetaResponse, local_ts_);
OB_SERIALIZE_MEMBER(ObHaGtsChangeMemberRequest, gts_id_, offline_replica_);
OB_SERIALIZE_MEMBER(ObHaGtsChangeMemberResponse, ret_value_);

OB_SERIALIZE_MEMBER(ObAdminAddDiskArg,
    diskgroup_name_, disk_path_, alias_name_, server_, zone_);
OB_SERIALIZE_MEMBER(ObAdminDropDiskArg,
    diskgroup_name_, alias_name_, server_, zone_);

void ObAlterTableResArg::reset()
{
  schema_type_ = OB_MAX_SCHEMA;
  schema_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
}

int ObDDLRes::assign(const ObDDLRes &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  schema_id_ = other.schema_id_;
  task_id_ = other.task_id_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLRes, tenant_id_, schema_id_, task_id_);

void ObAlterTableRes::reset()
{
  index_table_id_ = OB_INVALID_ID;
  constriant_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  res_arg_array_.reset();
  ddl_type_ = ObDDLType::DDL_INVALID;
  task_id_ = 0;
  ddl_res_array_.reset();
}

void ObDropDatabaseRes::reset()
{
  ddl_res_.reset();
  affected_row_ = 0;
}

int ObDropDatabaseRes::assign(const ObDropDatabaseRes &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_res_.assign(other.ddl_res_))) {
    LOG_WARN("assign ddl res failed", K(ret));
  } else {
    affected_row_ = other.affected_row_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDropDatabaseRes, ddl_res_, affected_row_);
OB_SERIALIZE_MEMBER(ObAlterTableResArg, schema_type_, schema_id_, schema_version_);
OB_SERIALIZE_MEMBER(ObAlterTableRes, index_table_id_, constriant_id_, schema_version_,
res_arg_array_, ddl_type_, task_id_, ddl_res_array_);
OB_SERIALIZE_MEMBER(ObGetTenantSchemaVersionArg, tenant_id_);
OB_SERIALIZE_MEMBER(ObGetTenantSchemaVersionResult, schema_version_);
OB_SERIALIZE_MEMBER(ObTenantMemoryArg, tenant_id_, memory_size_, refresh_interval_);
OB_SERIALIZE_MEMBER((ObKeystoreDDLArg, ObDDLArg), schema_, type_, is_kms_);

bool ObKeystoreDDLArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != schema_.get_tenant_id()
      && (CREATE_KEYSTORE == type_ ||
          ALTER_KEYSTORE_PASSWORD == type_ ||
          ALTER_KEYSTORE_SET_KEY == type_ ||
          ALTER_KEYSTORE_CLOSE == type_ ||
          ALTER_KEYSTORE_OPEN == type_ );
}

bool ObKeystoreDDLArg::is_allow_when_disable_ddl() const
{
  return OB_SYS_TENANT_ID == exec_tenant_id_ && CREATE_KEYSTORE == type_;
}

OB_SERIALIZE_MEMBER((ObLabelSePolicyDDLArg, ObDDLArg), ddl_type_, schema_);
OB_SERIALIZE_MEMBER((ObLabelSeComponentDDLArg, ObDDLArg), ddl_type_, schema_, policy_name_);
OB_SERIALIZE_MEMBER((ObLabelSeLabelDDLArg, ObDDLArg), ddl_type_, schema_, policy_name_);
OB_SERIALIZE_MEMBER((ObLabelSeUserLevelDDLArg, ObDDLArg), ddl_type_, level_schema_, policy_name_);
OB_SERIALIZE_MEMBER(ObCheckServerEmptyArg, mode_, sys_data_version_);
OB_SERIALIZE_MEMBER(ObCheckServerForAddingServerArg, mode_, sys_tenant_data_version_, server_id_);
int ObCheckServerForAddingServerArg::init(
    const Mode &mode,
    const uint64_t sys_tenant_data_version,
    const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  if (0 == sys_tenant_data_version || !is_valid_server_id(server_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(mode), K(sys_tenant_data_version), K(server_id));
  } else {
    mode_ = mode;
    sys_tenant_data_version_ = sys_tenant_data_version;
    server_id_ = server_id;
  }
  return ret;
}
int ObCheckServerForAddingServerArg::assign(const ObCheckServerForAddingServerArg &other) {
  int ret = OB_SUCCESS;
  mode_ = other.mode_;
  sys_tenant_data_version_ = other.sys_tenant_data_version_;
  server_id_ = other.server_id_;
  return ret;
}
bool ObCheckServerForAddingServerArg::is_valid() const
{
  return 0 != sys_tenant_data_version_ && is_valid_server_id(server_id_);
}
void ObCheckServerForAddingServerArg::reset()
{
  sys_tenant_data_version_ = 0;
  server_id_ = OB_INVALID_ID;
}
OB_SERIALIZE_MEMBER(
    ObCheckServerForAddingServerResult,
    is_server_empty_,
    zone_,
    sql_port_,
    build_version_);
int ObCheckServerForAddingServerResult::init(
    const bool is_server_empty,
    const ObZone &zone,
    const int64_t sql_port,
    const share::ObServerInfoInTable::ObBuildVersion &build_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty() || sql_port <= 0 || build_version.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(zone), K(sql_port), K(build_version));
  } else if (OB_FAIL(zone_.assign(zone))) {
    LOG_WARN("fail to assign zone", KR(ret), K(zone));
  } else if (OB_FAIL(build_version_.assign(build_version))) {
    LOG_WARN("fail to assign build version", KR(ret), K(build_version));
  } else {
    is_server_empty_ = is_server_empty;
    sql_port_ = sql_port;
  }
  return ret;
}
int ObCheckServerForAddingServerResult::assign(const ObCheckServerForAddingServerResult &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_.assign(other.zone_))) {
    LOG_WARN("fail to assign zone", KR(ret), K(other.zone_));
  } else if (OB_FAIL(build_version_.assign(other.build_version_))) {
    LOG_WARN("fail to assign build_version", KR(ret), K(other.build_version_));
  } else {
    is_server_empty_ = other.is_server_empty_;
    sql_port_ = other.sql_port_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObCheckDeploymentModeArg, single_zone_deployment_on_);
#ifdef OB_BUILD_TDE_SECURITY
OB_SERIALIZE_MEMBER(ObWaitMasterKeyInSyncArg,
                    tenant_max_key_version_,
                    tenant_config_version_,
                    rs_list_arg_);

int ObWaitMasterKeyInSyncArg::assign(
    const ObWaitMasterKeyInSyncArg &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_max_key_version_.assign(that.tenant_max_key_version_))) {
    LOG_WARN("fail to assign this member", KR(ret));
  } else if (OB_FAIL(tenant_config_version_.assign(that.tenant_config_version_))) {
    LOG_WARN("fail to assign this member", KR(ret));
  } else if (OB_FAIL(rs_list_arg_.assign(that.rs_list_arg_))) {
    LOG_WARN("fail to assign this member", KR(ret));
  }
  return ret;
}
#endif

OB_SERIALIZE_MEMBER(ObArchiveLogArg, enable_, tenant_id_, archive_tenant_ids_);
int ObArchiveLogArg::assign(const ObArchiveLogArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(archive_tenant_ids_.assign(other.archive_tenant_ids_))) {
    LOG_WARN("fail to assign archive_tenant_ids", K(ret));
  } else {
    enable_ = other.enable_;
    tenant_id_ = other.tenant_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSBackupCleanArg, trace_id_, job_id_, tenant_id_, incarnation_, task_id_, ls_id_, task_type_, id_, dest_id_, round_id_);

bool ObLSBackupCleanArg::is_valid() const
{
  return !trace_id_.is_invalid()
      && tenant_id_ > 0
      && task_id_ > 0
      && incarnation_ > 0
      && ls_id_.is_valid()
      && id_ > 0
      && dest_id_ >= 0
      && round_id_ >= 0
      && job_id_ > 0;
}

int ObLSBackupCleanArg::assign(const ObLSBackupCleanArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    job_id_ = arg.job_id_;
    trace_id_ = arg.trace_id_;
    tenant_id_ = arg.tenant_id_;
    task_id_ = arg.task_id_;
    task_type_ = arg.task_type_;
    ls_id_ = arg.ls_id_;
    id_ = arg.id_;
    dest_id_ = arg.dest_id_;
    round_id_ = arg.round_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupDataArg, trace_id_, job_id_, tenant_id_, task_id_, backup_set_id_,
    incarnation_id_, backup_type_, backup_date_, ls_id_, turn_id_, retry_id_, dst_server_, backup_path_, backup_data_type_);

bool ObBackupDataArg::is_valid() const
{
  return !trace_id_.is_invalid()
      && job_id_ > 0
      && tenant_id_ > 0
      && task_id_ > 0
      && backup_set_id_ > 0
      && incarnation_id_ > 0
      && backup_date_ > 0
      && ls_id_.is_valid()
      && turn_id_ > 0
      && retry_id_ >= 0
      && dst_server_.is_valid()
      && !backup_path_.is_empty()
      && backup_data_type_.is_valid();
}

int ObBackupDataArg::assign(const ObBackupDataArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(backup_path_.assign(arg.backup_path_.ptr()))) {
    LOG_WARN("fail to assign backup dest", K(ret));
  } else {
    trace_id_ = arg.trace_id_;
    job_id_ = arg.job_id_;
    tenant_id_ = arg.tenant_id_;
    task_id_ = arg.task_id_;
    backup_set_id_ = arg.backup_set_id_;
    incarnation_id_ = arg.incarnation_id_;
    backup_type_ = arg.backup_type_;
    backup_date_ = arg.backup_date_;
    ls_id_ = arg.ls_id_;
    turn_id_ = arg.turn_id_;
    retry_id_ = arg.retry_id_;
    dst_server_ = arg.dst_server_;
    backup_data_type_ = arg.backup_data_type_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupTaskRes, task_id_, job_id_, tenant_id_, ls_id_, src_server_, result_, trace_id_, dag_id_);

bool ObBackupTaskRes::is_valid() const
{
  return task_id_ > 0
      && job_id_ > 0
      && tenant_id_ > 0
      && src_server_.is_valid()
      && ls_id_.is_valid()
      && !trace_id_.is_invalid()
      && !dag_id_.is_invalid();
}

int ObBackupTaskRes::assign(const ObBackupTaskRes &res)
{
  int ret = OB_SUCCESS;
  if (!res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(res));
  } else {
    task_id_ = res.task_id_;
    job_id_ = res.job_id_;
    tenant_id_ = res.tenant_id_;
    ls_id_ = res.ls_id_;
    src_server_ = res.src_server_;
    result_ = res.result_;
    trace_id_ = res.trace_id_;
    dag_id_ = res.dag_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupComplLogArg, trace_id_, job_id_, tenant_id_, task_id_, backup_set_id_,
    incarnation_id_, backup_type_, backup_date_, ls_id_, dst_server_, backup_path_, start_scn_, end_scn_);

bool ObBackupComplLogArg::is_valid() const
{
  return !trace_id_.is_invalid()
      && job_id_ > 0
      && tenant_id_ > 0
      && task_id_ > 0
      && backup_set_id_ > 0
      && incarnation_id_ > 0
      && backup_date_ > 0
      && ls_id_.is_valid()
      && dst_server_.is_valid()
      && !backup_path_.is_empty()
      && start_scn_.is_valid()
      && end_scn_.is_valid();
}

int ObBackupComplLogArg::assign(const ObBackupComplLogArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(backup_path_.assign(arg.backup_path_.ptr()))) {
    LOG_WARN("fail to assgin backup dest", K(ret));
  } else {
    trace_id_ = arg.trace_id_;
    job_id_ = arg.job_id_;
    tenant_id_ = arg.tenant_id_;
    task_id_ = arg.task_id_;
    backup_set_id_ = arg.backup_set_id_;
    incarnation_id_ = arg.incarnation_id_;
    backup_type_ = arg.backup_type_;
    backup_date_ = arg.backup_date_;
    ls_id_ = arg.ls_id_;
    dst_server_ = arg.dst_server_;
    start_scn_ = arg.start_scn_;
    end_scn_ = arg.end_scn_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupBuildIdxArg, job_id_, task_id_, trace_id_, backup_path_, tenant_id_,
    backup_set_id_, incarnation_id_, backup_date_, backup_type_, turn_id_, retry_id_, start_turn_id_, dst_server_,
    backup_data_type_);

bool ObBackupBuildIdxArg::is_valid() const
{
  return !trace_id_.is_invalid()
      && job_id_ > 0
      && tenant_id_ > 0
      && task_id_ > 0
      && backup_set_id_ > 0
      && incarnation_id_ > 0
      && backup_date_ > 0
      && !backup_path_.is_empty()
      && turn_id_ > 0
      && retry_id_ >= 0
      && dst_server_.is_valid()
      && backup_data_type_.is_valid();
}

int ObBackupBuildIdxArg::assign(const ObBackupBuildIdxArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(backup_path_.assign(arg.backup_path_.ptr()))) {
    LOG_WARN("fail to assgin backup dest", K(ret));
  } else {
    job_id_ = arg.job_id_;
    task_id_ = arg.task_id_;
    trace_id_ = arg.trace_id_;
    tenant_id_ = arg.tenant_id_;
    backup_set_id_ = arg.backup_set_id_;
    incarnation_id_ = arg.incarnation_id_;
    backup_type_ = arg.backup_type_;
    backup_date_ = arg.backup_date_;
    turn_id_ = arg.turn_id_;
    retry_id_ = arg.retry_id_;
    start_turn_id_ = arg.start_turn_id_;
    dst_server_ = arg.dst_server_;
    backup_data_type_ = arg.backup_data_type_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupCheckTaskArg, tenant_id_, trace_id_);

bool ObBackupCheckTaskArg::is_valid() const
{
  return !trace_id_.is_invalid() && tenant_id_ != OB_INVALID_TENANT_ID;
}

int ObBackupCheckTaskArg::assign(const ObBackupCheckTaskArg& that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(that));
  } else {
    tenant_id_ = that.tenant_id_;
    trace_id_ = that.trace_id_;
  }
  return ret;
}


OB_SERIALIZE_MEMBER(ObBackupMetaArg, trace_id_, job_id_, tenant_id_, task_id_, backup_set_id_,
    incarnation_id_, backup_type_, backup_date_, ls_id_, turn_id_, retry_id_, start_scn_, dst_server_, backup_path_);

bool ObBackupMetaArg::is_valid() const
{
  return !trace_id_.is_invalid()
      && job_id_ > 0
      && tenant_id_ > 0
      && task_id_ > 0
      && backup_set_id_ > 0
      && incarnation_id_ > 0
      && backup_date_ > 0
      && ls_id_.is_valid()
      && turn_id_ > 0
      && retry_id_ >= 0
      && start_scn_.is_valid()
      && dst_server_.is_valid()
      && !backup_path_.is_empty();
}

int ObBackupMetaArg::assign(const ObBackupMetaArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(backup_path_.assign(arg.backup_path_.ptr()))) {
    LOG_WARN("fail to assign backup dest", K(ret));
  } else {
    trace_id_ = arg.trace_id_;
    job_id_ = arg.job_id_;
    tenant_id_ = arg.tenant_id_;
    task_id_ = arg.task_id_;
    backup_set_id_ = arg.backup_set_id_;
    incarnation_id_ = arg.incarnation_id_;
    backup_type_ = arg.backup_type_;
    backup_date_ = arg.backup_date_;
    ls_id_ = arg.ls_id_;
    turn_id_ = arg.turn_id_;
    retry_id_ = arg.retry_id_;
    start_scn_ = arg.start_scn_;
    dst_server_ = arg.dst_server_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupCheckTabletArg, tenant_id_, ls_id_, backup_scn_, tablet_ids_);

bool ObBackupCheckTabletArg::is_valid() const
{
  return tenant_id_ > 0 && ls_id_.is_valid() && !tablet_ids_.empty() && backup_scn_.is_valid();
}

int ObBackupCheckTabletArg::assign(const ObBackupCheckTabletArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(append(tablet_ids_, arg.tablet_ids_))) {
    LOG_WARN("fail to append tablet ids", K(ret));
  } else {
    tenant_id_ = arg.tenant_id_;
    backup_scn_ = arg.backup_scn_;
    ls_id_ = arg.ls_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupDatabaseArg, tenant_id_, initiator_tenant_id_, initiator_job_id_, backup_tenant_ids_, is_incremental_,
    is_compl_log_, backup_dest_, backup_description_, encryption_mode_, passwd_);

ObBackupDatabaseArg::ObBackupDatabaseArg()
	: tenant_id_(OB_INVALID_TENANT_ID),
    initiator_tenant_id_(OB_INVALID_TENANT_ID),
    initiator_job_id_(0),
    backup_tenant_ids_(),
    is_incremental_(false),
    is_compl_log_(false),
    backup_dest_(),
    backup_description_(),
    encryption_mode_(share::ObBackupEncryptionMode::NONE),
    passwd_()
{
}

bool ObBackupDatabaseArg::is_valid() const
{
  return share::ObBackupEncryptionMode::is_valid(encryption_mode_);
}

int  ObBackupDatabaseArg::assign(const ObBackupDatabaseArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(append(backup_tenant_ids_, arg.backup_tenant_ids_))) {
    LOG_WARN("fail to append backup tenant ids", K(ret));
  } else if (OB_FAIL(backup_dest_.assign(arg.backup_dest_))) {
    LOG_WARN("fail to assign backup dest", K(arg.backup_dest_));
  } else if (OB_FAIL(passwd_.assign(arg.passwd_))) {
    LOG_WARN("fail to assign passwd", K(ret), K(arg.passwd_));
  } else if (OB_FAIL(backup_description_.assign(arg.backup_description_))) {
    LOG_WARN("fail to assign backup_description_", K(ret));
  } else {
    tenant_id_ = arg.tenant_id_;
    initiator_tenant_id_ = arg.initiator_tenant_id_;
    initiator_job_id_ = arg.initiator_job_id_;
    is_incremental_ = arg.is_incremental_;
    is_compl_log_ = arg.is_compl_log_;
    encryption_mode_ = arg.encryption_mode_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupManageArg, tenant_id_, managed_tenant_ids_, type_, value_, copy_id_);

int ObBackupManageArg::assign(const ObBackupManageArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(managed_tenant_ids_, arg.managed_tenant_ids_))) {
    LOG_WARN("append managed_tenant_ids_ failed", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    type_ = arg.type_;
    value_ = arg.value_;
    copy_id_ = arg.copy_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupCleanArg, tenant_id_, initiator_tenant_id_, initiator_job_id_, type_, value_, dest_id_, description_, clean_tenant_ids_);
bool ObBackupCleanArg::is_valid() const
{
  return OB_INVALID_ID != initiator_tenant_id_ && value_ >= 0;
}

int ObBackupCleanArg::assign(const ObBackupCleanArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clean_tenant_ids_.assign(arg.clean_tenant_ids_))) {
    LOG_WARN("fail to assign clean_tenant_ids_", K(ret));
  } else {
    tenant_id_ = arg.tenant_id_;
    initiator_tenant_id_ = arg.initiator_tenant_id_;
    initiator_job_id_ = arg.initiator_job_id_;
    type_ = arg.type_;
    value_ = arg.value_;
    dest_id_ = arg.dest_id_;
    description_ = arg.description_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObNotifyArchiveArg, tenant_id_);

bool ObNotifyArchiveArg::is_valid() const
{
  return is_user_tenant(tenant_id_);
}

int ObNotifyArchiveArg::assign(const ObNotifyArchiveArg &arg)
{
  int ret = OB_SUCCESS;
  if(!arg.is_valid()){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDeletePolicyArg, initiator_tenant_id_, type_, policy_name_, recovery_window_,
    redundancy_, backup_copies_, clean_tenant_ids_);
bool ObDeletePolicyArg::is_valid() const
{
  return OB_INVALID_ID != initiator_tenant_id_ && 0 != strlen(policy_name_);
}

int ObDeletePolicyArg::assign(const ObDeletePolicyArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(policy_name_, sizeof(policy_name_), "%s", arg.policy_name_))) {
    LOG_WARN("fail to assign policy_name_", K(ret));
  } else if (OB_FAIL(databuff_printf(recovery_window_, sizeof(recovery_window_), "%s", arg.recovery_window_))) {
    LOG_WARN("fail to assign recovery_window_", K(ret));
  } else if (OB_FAIL(clean_tenant_ids_.assign(arg.clean_tenant_ids_))) {
    LOG_WARN("fail to assign clean_tenant_ids_", K(ret));
  } else {
    initiator_tenant_id_ = arg.initiator_tenant_id_;
    type_ = arg.type_;
    redundancy_ = arg.redundancy_;
    backup_copies_ = arg.backup_copies_;
  }

  return ret;
}

OB_SERIALIZE_MEMBER(CheckLeaderRpcIndex, switchover_timestamp_, epoch_,
                    ml_pk_index_, pkey_info_start_index_,
                    tenant_id_);
OB_SERIALIZE_MEMBER(ObBatchCheckRes, results_, index_);


bool CheckLeaderRpcIndex::is_valid() const
{
  return switchover_timestamp_ > 0 && epoch_ >= 0
      && ml_pk_index_ >= 0 && pkey_info_start_index_ >= 0
      && tenant_id_ > 0;
}

void CheckLeaderRpcIndex::reset()
{
  switchover_timestamp_ = 0;
  epoch_ = -1;
  ml_pk_index_ = -1;
  pkey_info_start_index_ = -1;
  tenant_id_ = OB_INVALID_TENANT_ID;
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

OB_SERIALIZE_MEMBER(ObPreProcessServerArg, server_, svr_seq_, rescue_server_);
int ObPreProcessServerArg::init(
    const common::ObAddr &server,
    const int64_t svr_seq,
    const common::ObAddr &rescue_server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid() || svr_seq <= 0 || !rescue_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(svr_seq), K(rescue_server));
  } else {
    server_ = server;
    svr_seq_ = svr_seq;
    rescue_server_ = rescue_server;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAdminRollingUpgradeArg, stage_);

bool ObAdminRollingUpgradeArg::is_valid() const
{
  return OB_UPGRADE_STAGE_DBUPGRADE <= stage_ && stage_ <= OB_UPGRADE_STAGE_POSTUPGRADE;
}

OB_SERIALIZE_MEMBER(ObRsListArg, rs_list_, master_rs_);

int ObRsListArg::assign(
    const ObRsListArg &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rs_list_.assign(that.rs_list_))) {
    LOG_WARN("fail to assign rs list", KR(ret));
  } else {
    master_rs_ = that.master_rs_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObFetchLocationArg, vtable_type_);

bool ObFetchLocationArg::is_valid() const
{
  return vtable_type_.is_valid();
}

int ObFetchLocationArg::init(const ObVtableLocationType &vtable_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!vtable_type.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or table_id", KR(ret), K(vtable_type));
  } else {
    vtable_type_ = vtable_type;
  }
  return ret;
}

int ObFetchLocationArg::assign(const ObFetchLocationArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(other.vtable_type_))) {
    LOG_WARN("fail to init arg", KR(ret), K(other));
  }
  return ret;
}
#ifdef OB_BUILD_TDE_SECURITY
OB_SERIALIZE_MEMBER(ObGetMasterKeyResultArg, str_);

OB_SERIALIZE_MEMBER(ObRestoreKeyArg, tenant_id_, backup_dest_, encrypt_key_);
int ObRestoreKeyArg::assign(const ObRestoreKeyArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  backup_dest_ = other.backup_dest_;
  encrypt_key_ = other.encrypt_key_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObRestoreKeyResult, ret_);
int ObRestoreKeyResult::assign(const ObRestoreKeyResult &other)
{
  int ret = OB_SUCCESS;
  ret_ = other.ret_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObRootKeyArg, tenant_id_, is_set_, key_type_, root_key_);

int ObRootKeyArg::init_for_get(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_set_ = false;
  }
  return ret;
}

int ObRootKeyArg::init(const uint64_t tenant_id, RootKeyType key_type,
            ObString &root_key)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)
      || RootKeyType::INVALID == key_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(key_type), K(root_key));
  } else {
    tenant_id_ = tenant_id;
    is_set_ = true;
    key_type_ = key_type;
    root_key_ = root_key;
  }
  return ret;
}

int ObRootKeyArg::assign(const ObRootKeyArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  is_set_ = other.is_set_;
  key_type_ = other.key_type_;
  root_key_ = other.root_key_;
  return ret;
}

OB_DEF_SERIALIZE(ObRootKeyResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              key_type_,
              root_key_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRootKeyResult)
{
  int ret = OB_SUCCESS;
  ObString tmp_str;
  LST_DO_CODE(OB_UNIS_DECODE,
              key_type_,
              tmp_str);
  if (OB_FAIL(ob_write_string(allocator_, tmp_str, root_key_))) {
    LOG_WARN("failed to copy string", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRootKeyResult)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              key_type_,
              root_key_);
  return len;
}

int ObRootKeyResult::assign(const ObRootKeyResult &other)
{
  int ret = OB_SUCCESS;
  key_type_ = other.key_type_;
  if (OB_FAIL(ob_write_string(allocator_, other.root_key_, root_key_))) {
    LOG_WARN("failed to write string", K(ret));
  }
  return ret;
}
#endif
OB_SERIALIZE_MEMBER(ObTrxToolArg, trans_id_, status_,
                    trans_version_, end_log_ts_, cmd_);
OB_SERIALIZE_MEMBER(ObTrxToolRes, trans_info_);
OB_SERIALIZE_MEMBER(ObPhysicalRestoreResult, job_id_, return_ret_, mod_, tenant_id_,
    trace_id_, addr_);

int ObTrxToolRes::reset()
{
  int ret = OB_SUCCESS;
  allocator_.reuse();
  buf_ = NULL;
  if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(BUF_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    trans_info_.assign_buffer(buf_, BUF_LENGTH);
  }
  return ret;
}

ObPhysicalRestoreResult::ObPhysicalRestoreResult()
  : job_id_(common::OB_INVALID_ID),
    return_ret_(common::OB_SUCCESS),
    mod_(share::PHYSICAL_RESTORE_MOD_MAX_NUM),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    trace_id_(),
    addr_()
{
}

bool ObPhysicalRestoreResult::is_valid() const
{
  return job_id_ > 0
         && OB_SUCCESS != return_ret_
         && mod_ >= share::PHYSICAL_RESTORE_MOD_RS
         && mod_ < share::PHYSICAL_RESTORE_MOD_MAX_NUM
         && addr_.is_valid();
}

int ObPhysicalRestoreResult::assign(const ObPhysicalRestoreResult &other)
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

OB_SERIALIZE_MEMBER(ObRefreshTimezoneArg, tenant_id_);
OB_SERIALIZE_MEMBER(ObCreateRestorePointArg,
                   tenant_id_,
                   name_);
OB_SERIALIZE_MEMBER(ObDropRestorePointArg,
                   tenant_id_,
                   name_);

OB_SERIALIZE_MEMBER(ObCheckBuildIndexTaskExistArg,
                    tenant_id_, task_id_, scheduler_id_);
OB_SERIALIZE_MEMBER(ObLogReqLoadProxyRequest, agency_addr_seq_, principal_addr_seq_, principal_crashed_ts_);
OB_SERIALIZE_MEMBER(ObLogReqLoadProxyResponse, err_);
OB_SERIALIZE_MEMBER(ObLogReqUnloadProxyRequest, agency_addr_seq_, principal_addr_seq_);
OB_SERIALIZE_MEMBER(ObLogReqUnloadProxyResponse, err_);
OB_SERIALIZE_MEMBER(ObLogReqLoadProxyProgressRequest, agency_addr_seq_, principal_addr_seq_);
OB_SERIALIZE_MEMBER(ObLogReqLoadProxyProgressResponse, err_, progress_);

OB_SERIALIZE_MEMBER(ObDDLBuildSingleReplicaRequestArg, tenant_id_, ls_id_, source_tablet_id_, dest_tablet_id_,
  source_table_id_, dest_schema_id_, schema_version_, snapshot_version_, ddl_type_, task_id_,
  execution_id_, parallelism_, tablet_task_id_, data_format_version_, consumer_group_id_, dest_tenant_id_, dest_ls_id_, dest_schema_version_);

int ObDDLBuildSingleReplicaRequestArg::assign(const ObDDLBuildSingleReplicaRequestArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  ls_id_ = other.ls_id_;
  dest_tenant_id_ = other.dest_tenant_id_;
  dest_ls_id_ = other.dest_ls_id_;
  source_tablet_id_ = other.source_tablet_id_;
  dest_tablet_id_ = other.dest_tablet_id_;
  source_table_id_ = other.source_table_id_;
  dest_schema_id_ = other.dest_schema_id_;
  schema_version_ = other.schema_version_;
  dest_schema_version_ = other.dest_schema_version_;
  snapshot_version_ = other.snapshot_version_;
  ddl_type_ = other.ddl_type_;
  task_id_ = other.task_id_;
  parallelism_ = other.parallelism_;
  execution_id_ = other.execution_id_;
  tablet_task_id_ = other.tablet_task_id_;
  data_format_version_ = other.data_format_version_;
  consumer_group_id_ = other.consumer_group_id_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLBuildSingleReplicaRequestResult, ret_code_, row_inserted_, row_scanned_);

int ObDDLBuildSingleReplicaRequestResult::assign(const ObDDLBuildSingleReplicaRequestResult &other)
{
  int ret = OB_SUCCESS;
  ret_code_ = other.ret_code_;
  row_inserted_ = other.row_inserted_;
  row_scanned_ = other.row_scanned_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLBuildSingleReplicaResponseArg, tenant_id_, ls_id_, tablet_id_,
                    source_table_id_, dest_schema_id_, ret_code_, snapshot_version_, schema_version_,
                    task_id_, execution_id_, row_scanned_, row_inserted_, dest_tenant_id_, dest_ls_id_, dest_schema_version_,
                    server_addr_);

int ObDDLBuildSingleReplicaResponseArg::assign(const ObDDLBuildSingleReplicaResponseArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  dest_tenant_id_ = other.dest_tenant_id_;
  ls_id_ = other.ls_id_;
  dest_ls_id_ = other.dest_ls_id_;
  tablet_id_ = other.tablet_id_;
  source_table_id_ = other.source_table_id_;
  dest_schema_id_ = other.dest_schema_id_;
  ret_code_ = other.ret_code_;
  snapshot_version_ = other.snapshot_version_;
  schema_version_ = other.schema_version_;
  dest_schema_version_ = other.dest_schema_version_;
  task_id_ = other.task_id_;
  execution_id_ = other.execution_id_;
  row_scanned_ = other.row_scanned_;
  row_inserted_ = other.row_inserted_;
  server_addr_ = other.server_addr_;
  return ret;
}

int ObCreateDirectoryArg::assign(const ObCreateDirectoryArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(schema_.assign(other.schema_))) {
    LOG_WARN("fail to assign directory schema", KR(ret));
  } else {
    or_replace_ = other.or_replace_;
    user_id_ = other.user_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObCreateDirectoryArg, ObDDLArg), or_replace_, user_id_, schema_);

int ObDropDirectoryArg::assign(const ObDropDirectoryArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    directory_name_ = other.directory_name_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDropDirectoryArg, ObDDLArg), tenant_id_, directory_name_);


#ifdef OB_BUILD_TDE_SECURITY
int ObDumpCacheMasterKeyResultArg::ObMasterKeyVersionPair::assign(
    const ObDumpCacheMasterKeyResultArg::ObMasterKeyVersionPair &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  master_key_version_ = other.master_key_version_;
  master_key_.assign_buffer(buf_, share::OB_CLOG_ENCRYPT_MASTER_KEY_LEN);
  master_key_.write(other.master_key_.ptr(), other.master_key_.length());
  return ret;
}

OB_SERIALIZE_MEMBER(ObDumpCacheMasterKeyResultArg, master_key_version_array_);
OB_SERIALIZE_MEMBER(ObDumpCacheMasterKeyResultArg::ObMasterKeyVersionPair,
                    tenant_id_, master_key_version_, master_key_);
#endif

int ObCreateDupLSArg::assign(const ObCreateDupLSArg &arg)
{
  int ret = OB_SUCCESS;
  tenant_id_ = arg.tenant_id_;
  return ret;
}

int ObCreateDupLSArg::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

DEF_TO_STRING(ObCreateDupLSArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id));
  return pos;
}

OB_SERIALIZE_MEMBER(ObCreateDupLSArg, tenant_id_);

bool ObCreateLSArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && id_.is_valid()
         && REPLICA_TYPE_MAX != replica_type_
         && replica_property_.is_valid()
         && tenant_info_.is_valid()
         && create_scn_.is_valid()
         && lib::Worker::CompatMode::INVALID != compat_mode_
         && (!is_create_ls_with_palf()
             || palf_base_info_.is_valid());
}
void ObCreateLSArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  id_.reset();
  replica_type_ = REPLICA_TYPE_MAX;
  replica_property_.reset();
  tenant_info_.reset();
  create_scn_.reset();
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  create_ls_type_ = EMPTY_LS;
  palf_base_info_.reset();
}

int ObCreateLSArg::assign(const ObCreateLSArg &arg)
{
  int ret = OB_SUCCESS;
  if (this == &arg) {
  } else {
    (void)tenant_info_.assign(arg.tenant_info_);
    tenant_id_ = arg.tenant_id_;
    id_ = arg.id_;
    replica_type_ = arg.replica_type_;
    replica_property_ = arg.replica_property_;
    create_scn_ = arg.create_scn_;
    compat_mode_ = arg.compat_mode_;
    create_ls_type_ = arg.create_ls_type_;
    palf_base_info_ = arg.palf_base_info_;
  }
  return ret;
}

int ObCreateLSArg::init(const int64_t tenant_id,
    const share::ObLSID &id,  const ObReplicaType replica_type,
    const common::ObReplicaProperty &replica_property,
    const share::ObAllTenantInfo &tenant_info,
    const SCN &create_scn,
    const lib::Worker::CompatMode &mode,
    const bool create_with_palf,
    const palf::PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  ||!id.is_valid()
                  || REPLICA_TYPE_MAX == replica_type
                  || !replica_property.is_valid()
                  || !tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(id), K(replica_type), K(tenant_info));
  } else {
    (void)tenant_info_.assign(tenant_info);
    tenant_id_ = tenant_id;
    id_ = id;
    replica_type_ = replica_type;
    replica_property_ = replica_property;
    create_scn_ = create_scn;
    compat_mode_ = mode;
    if (create_with_palf) {
      create_ls_type_ = CREATE_WITH_PALF;
    } else {
      create_ls_type_ = EMPTY_LS;
    }
    palf_base_info_ = palf_base_info;
  }
  return ret;
}

DEF_TO_STRING(ObCreateLSArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(id), K_(replica_type), K_(replica_property), K_(tenant_info),
       K_(create_scn), K_(compat_mode), K_(palf_base_info), "create with palf", is_create_ls_with_palf());
  return pos;
}

OB_SERIALIZE_MEMBER(ObCreateLSArg, tenant_id_, id_, replica_type_, replica_property_, tenant_info_,
create_scn_, compat_mode_, create_ls_type_, palf_base_info_);

#ifdef OB_BUILD_ARBITRATION
bool ObCreateArbArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid()
         && tenant_role_.is_valid()
         && ls_id_.is_valid_with_tenant(tenant_id_);
}

void ObCreateArbArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  tenant_role_.reset();
}

int ObCreateArbArg::assign(const ObCreateArbArg &arg)
{
  int ret = OB_SUCCESS;
  if (this == &arg) {
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    tenant_role_ = arg.tenant_role_;
  }
  return ret;
}

int ObCreateArbArg::init(
    const int64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::ObTenantRole &tenant_role)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid()
                  || !ls_id.is_valid_with_tenant(tenant_id)
                  || !tenant_role.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(tenant_role));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tenant_role_ = tenant_role;
  }
  return ret;
}

DEF_TO_STRING(ObCreateArbArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(ls_id), K_(tenant_role));
  return pos;
}

OB_SERIALIZE_MEMBER(ObCreateArbArg, tenant_id_, ls_id_, tenant_role_);

bool ObDeleteArbArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid()
         && ls_id_.is_valid_with_tenant(tenant_id_);
}

void ObDeleteArbArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
}

int ObDeleteArbArg::assign(const ObDeleteArbArg &arg)
{
  int ret = OB_SUCCESS;
  if (this == &arg) {
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
  }
  return ret;
}

int ObDeleteArbArg::init(
    const int64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid()
                  || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
  }
  return ret;
}

DEF_TO_STRING(ObDeleteArbArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(ls_id));
  return pos;
}

OB_SERIALIZE_MEMBER(ObDeleteArbArg, tenant_id_, ls_id_);
#endif

bool ObSetMemberListArgV2::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && id_.is_valid()
         && member_list_.is_valid()
         && paxos_replica_num_ >= member_list_.get_member_number();
}

void ObSetMemberListArgV2::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  id_.reset();
  member_list_.reset();
  paxos_replica_num_ = 0;
  arbitration_service_.reset();
  learner_list_.reset();
}

int ObSetMemberListArgV2::assign(const ObSetMemberListArgV2 &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (OB_FAIL(member_list_.deep_copy(arg.member_list_))) {
    LOG_WARN("failed to assign member list", KR(ret), K(arg));
  } else if (OB_FAIL(learner_list_.deep_copy(arg.learner_list_))) {
    LOG_WARN("failed to assign learner list", KR(ret), K(arg));
  } else if (OB_FAIL(arbitration_service_.assign(arg.arbitration_service_))) {
    LOG_WARN("failed to assign arbitration_service", KR(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    id_ = arg.id_;
    paxos_replica_num_ = arg.paxos_replica_num_;
  }
  return ret;
}

int ObSetMemberListArgV2::init(const int64_t tenant_id,
    const share::ObLSID &id, const int64_t paxos_replica_num,
    const ObMemberList &member_list, const ObMember &arbitration_service,
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !id.is_valid()
                  || !member_list.is_valid()
                  || member_list.get_member_number() > paxos_replica_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(id), K(member_list), K(paxos_replica_num));
  } else if (OB_FAIL(member_list_.deep_copy(member_list))) {
    LOG_WARN("failed to assign member list", KR(ret), K(member_list));
  } else if (OB_FAIL(learner_list_.deep_copy(learner_list))) {
    LOG_WARN("fail ed to assign learner list", KR(ret), K(learner_list));
  } else if (OB_FAIL(arbitration_service_.assign(arbitration_service))) {
    LOG_WARN("failed to assign arbitration service", KR(ret), K(arbitration_service));
  } else {
    tenant_id_ = tenant_id;
    id_ = id;
    paxos_replica_num_ = paxos_replica_num;
  }
  return ret;
}

DEF_TO_STRING(ObSetMemberListArgV2)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(id), K_(paxos_replica_num), K_(member_list), K_(arbitration_service), K_(learner_list));
  return pos;
}

OB_SERIALIZE_MEMBER(ObSetMemberListArgV2, tenant_id_, id_, member_list_, paxos_replica_num_, arbitration_service_, learner_list_);

bool ObGetLSAccessModeInfoArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid();
}
int ObGetLSAccessModeInfoArg::init(
    uint64_t tenant_id, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
  }
  return ret;
}

int ObGetLSAccessModeInfoArg::assign(const ObGetLSAccessModeInfoArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObGetLSAccessModeInfoArg, tenant_id_, ls_id_);

bool ObLSAccessModeInfo::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && ls_id_.is_valid()
      && palf::INVALID_PROPOSAL_ID != mode_version_
      && palf::AccessMode::INVALID_ACCESS_MODE != access_mode_
      && ref_scn_.is_valid();
    // only check sys_ls_end_scn when it's needed
}
int ObLSAccessModeInfo::init(
    uint64_t tenant_id, const ObLSID &ls_id,
    const int64_t mode_version,
    const palf::AccessMode &access_mode,
    const SCN &ref_scn,
    const share::SCN &sys_ls_end_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid()
                  || palf::AccessMode::INVALID_ACCESS_MODE == access_mode
                  || palf::INVALID_PROPOSAL_ID == mode_version
                  || !ref_scn.is_valid())) {
    // only check sys_ls_end_scn when it's needed
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id),
    K(mode_version), K(access_mode), K(ref_scn), K(sys_ls_end_scn));

  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    mode_version_ = mode_version;
    access_mode_ = access_mode;
    ref_scn_ = ref_scn;
    sys_ls_end_scn_ = sys_ls_end_scn;
  }
  return ret;
}
int ObLSAccessModeInfo::assign(const ObLSAccessModeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    mode_version_ = other.mode_version_;
    access_mode_ = other.access_mode_;
    ref_scn_ = other.ref_scn_;
    sys_ls_end_scn_ = other.sys_ls_end_scn_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObLSAccessModeInfo, tenant_id_, ls_id_, mode_version_, access_mode_, ref_scn_, addr_, sys_ls_end_scn_);

bool ObChangeLSAccessModeRes::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid();
}
int ObChangeLSAccessModeRes::init(
    uint64_t tenant_id, const ObLSID &ls_id,
    int result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    ret_ = result;
  }
  return ret;
}

int ObChangeLSAccessModeRes::assign(const ObChangeLSAccessModeRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    ret = other.ret_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObChangeLSAccessModeRes, tenant_id_, ls_id_, ret_);

int ObNotifySwitchLeaderArg::init(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObAddr &leader, const SwitchLeaderComment &comment)
{
  int ret = OB_SUCCESS;
  if (ObNotifySwitchLeaderArg::INVALID_COMMENT == comment) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(comment));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    advise_leader_ = leader;
    comment_ = comment;
  }
  return ret;
}

int ObNotifySwitchLeaderArg::assign(const ObNotifySwitchLeaderArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  ls_id_ = other.ls_id_;
  advise_leader_ = other.advise_leader_;
  comment_ = other.comment_;
  return ret;
}

const char* SwitchLeaderCommentStr[] =
{
  "STOP SERVER", "STOP ZONE", "CHANGE MEMBER", "MODIFY PRIMARY_ZONE", "MANUAL SWITCH", "CREATE LS"
};
const char* ObNotifySwitchLeaderArg::comment_to_str() const
{
  const char* str = "UNKNOWN";
  if (ObNotifySwitchLeaderArg::INVALID_COMMENT == comment_) {
    LOG_WARN_RET(OB_NOT_INIT, "comment is invalid", K(comment_));
  } else {
    str = SwitchLeaderCommentStr[comment_];
  }
  return str;
}

OB_SERIALIZE_MEMBER(ObNotifySwitchLeaderArg, tenant_id_, ls_id_, advise_leader_, comment_);

bool ObBatchRemoveTabletArg::is_valid() const
{
  bool is_valid = id_.is_valid();
  for (int64_t i = 0; i < tablet_ids_.count() && is_valid; i++) {
    is_valid = tablet_ids_.at(i).is_valid();
  }
  return is_valid;
}

void ObBatchRemoveTabletArg::reset()
{
  tablet_ids_.reset();
  id_.reset();
  is_old_mds_ = false;
}

int ObBatchRemoveTabletArg::assign(const ObBatchRemoveTabletArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_ids_.assign(arg.tablet_ids_))) {
    LOG_WARN("failed to assign table ids", KR(ret), K(arg));
  } else {
    id_ = arg.id_;
    is_old_mds_ = arg.is_old_mds_;
  }
  return ret;
}

int ObBatchRemoveTabletArg::init(const ObIArray<common::ObTabletID> &tablet_ids,
                          const share::ObLSID id)
{
  int ret = OB_SUCCESS;
  bool is_valid = id.is_valid();
  for (int64_t i = 0; i < tablet_ids.count() && is_valid; i++) {
    is_valid = tablet_ids.at(i).is_valid();
  }
  if (OB_UNLIKELY(!is_valid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_ids), K(id));
  } else if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign table schema index", KR(ret), K(tablet_ids));
  } else {
    id_ = id;
  }
  return ret;
}

DEF_TO_STRING(ObBatchRemoveTabletArg)
{
  int64_t pos = 0;
  J_KV(K_(id), K_(tablet_ids), K_(is_old_mds));
  return pos;
}

OB_DEF_SERIALIZE(ObBatchRemoveTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tablet_ids_, id_, is_old_mds_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBatchRemoveTabletArg)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tablet_ids_, id_, is_old_mds_);
  return len;
}

OB_DEF_DESERIALIZE(ObBatchRemoveTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tablet_ids_, id_);
  if (OB_SUCC(ret)) {
    if (pos == data_len) {
      is_old_mds_ = true;
    } else {
      LST_DO_CODE(OB_UNIS_DECODE, is_old_mds_);
    }
  }
  return ret;
}

// ----------------------
bool ObCreateTabletInfo::is_valid() const
{
  bool is_valid = data_tablet_id_.is_valid()
                  && table_schema_index_.count() > 0
                  && table_schema_index_.count() == tablet_ids_.count()
                  && lib::Worker::CompatMode::INVALID != compat_mode_;
  for (int64_t i = 0; i < tablet_ids_.count() && is_valid; i++) {
    is_valid = tablet_ids_.at(i).is_valid();
  }
  return is_valid;
}

void ObCreateTabletInfo::reset()
{
  tablet_ids_.reset();
  data_tablet_id_.reset();
  table_schema_index_.reset();
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  is_create_bind_hidden_tablets_ = false;
}

int ObCreateTabletInfo::assign(const ObCreateTabletInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is invalid", KR(ret), K(info));
  } else if (OB_FAIL(tablet_ids_.assign(info.tablet_ids_))) {
    LOG_WARN("failed to assign table ids", KR(ret), K(info));
  } else if (OB_FAIL(table_schema_index_.assign(info.table_schema_index_))) {
    LOG_WARN("failed to assign table schema index", KR(ret), K(info));
  } else {
    data_tablet_id_ = info.data_tablet_id_;
    compat_mode_ = info.compat_mode_;
    is_create_bind_hidden_tablets_ = info.is_create_bind_hidden_tablets_;
  }
  return ret;
}

int ObCreateTabletInfo::init(const ObIArray<common::ObTabletID> &tablet_ids,
                             common::ObTabletID data_tablet_id,
                             const common::ObIArray<int64_t> &table_schema_index,
                             const lib::Worker::CompatMode &mode,
                             const bool is_create_bind_hidden_tablets)
{
  int ret = OB_SUCCESS;
  bool is_valid = data_tablet_id.is_valid()
                  // && OB_INVALID_VERSION != schema_version
                  && table_schema_index.count() > 0
                  && table_schema_index.count() == tablet_ids.count();
  for (int64_t i = 0; i < tablet_ids.count() && is_valid; i++) {
    is_valid = tablet_ids.at(i).is_valid();
  }
  if (OB_UNLIKELY(!is_valid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_ids), K(data_tablet_id), K(table_schema_index));
  } else if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign table schema index", KR(ret), K(table_schema_index));
  } else if (OB_FAIL(table_schema_index_.assign(table_schema_index))) {
    LOG_WARN("failed to assign table schema index", KR(ret), K(table_schema_index));
  } else {
    data_tablet_id_ = data_tablet_id;
    compat_mode_ = mode;
    is_create_bind_hidden_tablets_ = is_create_bind_hidden_tablets;
  }
  return ret;
}

DEF_TO_STRING(ObCreateTabletInfo)
{
  int64_t pos = 0;
  J_KV(K_(tablet_ids), K_(data_tablet_id), K_(table_schema_index), K_(compat_mode), K_(is_create_bind_hidden_tablets));
  return pos;
}

OB_SERIALIZE_MEMBER(ObCreateTabletInfo, tablet_ids_, data_tablet_id_, table_schema_index_, compat_mode_, is_create_bind_hidden_tablets_);

bool ObBatchCreateTabletArg::is_inited() const
{
  return id_.is_valid() && major_frozen_scn_.is_valid();
}

bool ObBatchCreateTabletArg::is_valid() const
{
  bool valid = true;
  if (is_inited() && tablets_.count() > 0 && table_schemas_.count() > 0) {
    for (int64_t i = 0; valid && i < tablets_.count(); ++i) {
      const ObCreateTabletInfo &info = tablets_[i];
      const common::ObSArray<int64_t> &table_schema_index = info.table_schema_index_;
      if (!info.is_valid()) {
        valid = false;
      }

      for (int64_t j = 0; valid && j < table_schema_index.count(); ++j) {
        const int64_t index = table_schema_index[j];
        if (index < 0 || index >= table_schemas_.count()) {
          valid = false;
        }
      }
    }
  } else {
    valid = false;
  }
  return valid;
}

void ObBatchCreateTabletArg::reset()
{
  id_.reset();
  major_frozen_scn_.reset();
  tablets_.reset();
  table_schemas_.reset();
  need_check_tablet_cnt_ = false;
  is_old_mds_ = false;
}

int ObBatchCreateTabletArg::assign(const ObBatchCreateTabletArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (OB_FAIL(tablets_.assign(arg.tablets_))) {
    LOG_WARN("failed to assign tablets", KR(ret), K(arg));
  } else if (OB_FAIL(table_schemas_.assign(arg.table_schemas_))) {
    LOG_WARN("failed to assign table schema", KR(ret), K(arg));
  } else {
    id_ = arg.id_;
    major_frozen_scn_ = arg.major_frozen_scn_;
    need_check_tablet_cnt_ = arg.need_check_tablet_cnt_;
    is_old_mds_ = arg.is_old_mds_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObContextDDLArg, ObDDLArg),
                    stmt_type_,
                    ctx_schema_,
                    or_replace_);

int ObContextDDLArg::assign(const ObContextDDLArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(ctx_schema_.assign(other.ctx_schema_))) {
    LOG_WARN("fail to assign context schema", KR(ret));
  } else {
    stmt_type_ = other.stmt_type_;
    or_replace_ = other.or_replace_;
  }
  return ret;
}

int ObBatchCreateTabletArg::init_create_tablet(
  const share::ObLSID &id,
  const SCN &major_frozen_scn,
  const bool need_check_tablet_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid() || !major_frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(id), K(major_frozen_scn));
  } else {
    id_ = id;
    major_frozen_scn_ = major_frozen_scn;
    need_check_tablet_cnt_ = need_check_tablet_cnt;
  }
  return ret;
}

int64_t ObBatchCreateTabletArg::get_tablet_count() const
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < tablets_.count(); ++i) {
    const ObCreateTabletInfo &info = tablets_[i];
    cnt += info.get_tablet_count();
  }
  return cnt;
}

DEF_TO_STRING(ObBatchCreateTabletArg)
{
  int64_t pos = 0;
  J_KV(K_(id), K_(major_frozen_scn), K_(need_check_tablet_cnt), K_(is_old_mds), K_(tablets));
  return pos;
}

OB_DEF_SERIALIZE(ObBatchCreateTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, id_, major_frozen_scn_, tablets_, table_schemas_, need_check_tablet_cnt_, is_old_mds_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBatchCreateTabletArg)
{
  int len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, id_, major_frozen_scn_, tablets_, table_schemas_, need_check_tablet_cnt_, is_old_mds_);
  return len;
}

OB_DEF_DESERIALIZE(ObBatchCreateTabletArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, id_, major_frozen_scn_, tablets_, table_schemas_, need_check_tablet_cnt_);
  if (OB_SUCC(ret)) {
    if (pos == data_len) {
      is_old_mds_ = true;
    } else {
      LST_DO_CODE(OB_UNIS_DECODE, is_old_mds_);
    }
  }
  return ret;
}



OB_SERIALIZE_MEMBER(ObCreateDupLSResult, ret_);
bool ObCreateDupLSResult::is_valid() const
{
  return true;
}
int ObCreateDupLSResult::assign(const ObCreateDupLSResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ret_ = other.ret_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCreateLSResult, ret_, addr_, replica_type_);
bool ObCreateLSResult::is_valid() const
{
  return true;
}
int ObCreateLSResult::assign(const ObCreateLSResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ret_ = other.ret_;
    addr_ = other.addr_;
    replica_type_ = other.replica_type_;
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
OB_SERIALIZE_MEMBER(ObCreateArbResult, ret_);
bool ObCreateArbResult::is_valid() const
{
  return true;
}

int ObCreateArbResult::assign(const ObCreateArbResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ret_ = other.ret_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDeleteArbResult, ret_);
bool ObDeleteArbResult::is_valid() const
{
  return true;
}

int ObDeleteArbResult::assign(const ObDeleteArbResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ret_ = other.ret_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAddArbResult, ret_);
bool ObAddArbResult::is_valid() const
{
  return true;
}

int ObAddArbResult::assign(const ObAddArbResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ret_ = other.ret_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRemoveArbResult, ret_);
bool ObRemoveArbResult::is_valid() const
{
  return true;
}

int ObRemoveArbResult::assign(const ObRemoveArbResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ret_ = other.ret_;
  }
  return ret;
}
#endif
OB_SERIALIZE_MEMBER(ObSetMemberListResult, ret_);
bool ObSetMemberListResult::is_valid() const
{
  return true;
}
int ObSetMemberListResult::assign(const ObSetMemberListResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ret_ = other.ret_;
  }
  return ret;
}

bool ObFetchTabletSeqArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && tablet_id_.is_valid();
}

void ObFetchTabletSeqArg::reset()
{
  tablet_id_.reset();
  ls_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  cache_size_ = 0;
}

int ObFetchTabletSeqArg::assign(const ObFetchTabletSeqArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else {
    tablet_id_ = arg.tablet_id_;
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    cache_size_ = arg.cache_size_;
  }
  return ret;
}

int ObFetchTabletSeqArg::init(const uint64_t tenant_id,
                              const uint64_t cache_size,
                              const ObTabletID tablet_id,
                              const share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || 0 >= cache_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(cache_size), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    tenant_id_ = tenant_id;
    cache_size_ = cache_size;
    ls_id_ = ls_id;
  }
  return ret;
}

DEF_TO_STRING(ObFetchTabletSeqArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(tablet_id), K_(ls_id));
  return pos;
}

OB_SERIALIZE_MEMBER(ObFetchTabletSeqArg, tenant_id_, cache_size_, tablet_id_, ls_id_);

bool ObFetchTabletSeqRes::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && cache_interval_.is_valid();
}

void ObFetchTabletSeqRes::reset()
{
  cache_interval_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObFetchTabletSeqRes::assign(const ObFetchTabletSeqRes &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else {
    cache_interval_ = arg.cache_interval_;
    tenant_id_ = arg.tenant_id_;
  }
  return ret;
}

int ObFetchTabletSeqRes::init(const uint64_t tenant_id, const ObTabletAutoincInterval cache_interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !cache_interval.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(cache_interval));
  } else {
    cache_interval_ = cache_interval;
    tenant_id_ = tenant_id;
  }
  return ret;
}

DEF_TO_STRING(ObFetchTabletSeqRes)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(cache_interval));
  return pos;
}

OB_SERIALIZE_MEMBER(ObFetchTabletSeqRes, tenant_id_, cache_interval_);

OB_SERIALIZE_MEMBER(ObDetectMasterRsArg, addr_, cluster_id_);
ObDetectMasterRsArg::ObDetectMasterRsArg()
    : addr_(), cluster_id_(common::OB_INVALID_ID)
{}

ObDetectMasterRsArg::~ObDetectMasterRsArg()
{}

void ObDetectMasterRsArg::reset()
{
  addr_.reset();
  cluster_id_ = common::OB_INVALID_ID;
}

int ObDetectMasterRsArg::assign(const ObDetectMasterRsArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    addr_ = other.addr_;
    cluster_id_ = other.cluster_id_;
  }
  return ret;
}

bool ObDetectMasterRsArg::is_valid() const
{
  return addr_.is_valid() && common::OB_INVALID_ID != cluster_id_;
}

int ObDetectMasterRsArg::init(
    const ObAddr &addr,
    const int64_t &cluster_id)
{
  int ret = OB_SUCCESS;
  addr_ = addr;
  cluster_id_ = cluster_id;
  return ret;
}

void ObDetectMasterRsArg::set_addr(const ObAddr &addr)
{
  addr_ = addr;
}

void ObDetectMasterRsArg::set_cluster_id(const int64_t &cluster_id)
{
  cluster_id_ = cluster_id;
}

ObAddr ObDetectMasterRsArg::get_addr() const
{
  return addr_;
}

int64_t ObDetectMasterRsArg::get_cluster_id() const
{
  return cluster_id_;
}

OB_SERIALIZE_MEMBER(ObDetectMasterRsLSResult, role_, master_rs_, replica_, ls_info_);
ObDetectMasterRsLSResult::ObDetectMasterRsLSResult()
    : role_(ObRole::INVALID_ROLE),
      master_rs_(),
      replica_(),
      ls_info_()
{}

ObDetectMasterRsLSResult::~ObDetectMasterRsLSResult()
{}

int ObDetectMasterRsLSResult::assign(const ObDetectMasterRsLSResult &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(replica_.assign(other.replica_))) {
    LOG_WARN("fail to assign replica", KR(ret), K_(other.replica));
  } else if (OB_FAIL(ls_info_.assign(other.ls_info_))) {
    LOG_WARN("fail to assign ls info", KR(ret), K_(other.ls_info));
  }
  return ret;
}

int ObDetectMasterRsLSResult::init(
    const common::ObRole &role,
    const common::ObAddr &master_rs,
    const share::ObLSReplica &replica,
    const share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replica_.assign(replica))) {
    LOG_WARN("fail to assign replica", KR(ret), K(replica));
  } else if (OB_FAIL(ls_info_.assign(ls_info))) {
    LOG_WARN("fail to assign ls info", KR(ret), K(ls_info));
  } else {
    role_ = role;
    master_rs_ = master_rs;
  }
  return ret;
}

void ObDetectMasterRsLSResult::reset()
{
  role_ = ObRole::INVALID_ROLE;
  master_rs_.reset();
  replica_.reset();
  ls_info_.reset();
}

void ObDetectMasterRsLSResult::set_role(const common::ObRole &role)
{
  role_ = role;
}

void ObDetectMasterRsLSResult::set_master_rs(const common::ObAddr &master_rs)
{
  master_rs_ = master_rs;
}

int ObDetectMasterRsLSResult::set_replica(const share::ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replica_.assign(replica))) {
    LOG_WARN("fail to assign replica", KR(ret), K(replica));
  }
  return ret;
}

int ObDetectMasterRsLSResult::set_ls_info(const share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_info_.assign(ls_info))) {
    LOG_WARN("fail to assign ls info", KR(ret), K(ls_info));
  }
  return ret;
}

ObRole ObDetectMasterRsLSResult::get_role() const
{
  return role_;
}

ObAddr ObDetectMasterRsLSResult::get_master_rs() const
{
  return master_rs_;
}

const ObLSReplica &ObDetectMasterRsLSResult::get_replica() const
{
  return replica_;
}

const ObLSInfo &ObDetectMasterRsLSResult::get_ls_info() const
{
  return ls_info_;
}

ObBatchBroadcastSchemaArg::ObBatchBroadcastSchemaArg()
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    sys_schema_version_(common::OB_INVALID_VERSION),
    tables_()
{}

ObBatchBroadcastSchemaArg::~ObBatchBroadcastSchemaArg()
{}

int ObBatchBroadcastSchemaArg::init(
  const uint64_t tenant_id,
  const int64_t sys_schema_version,
  const common::ObIArray<share::schema::ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tables_.assign(tables))) {
    LOG_WARN("fail to assign tables", KR(ret), K(tenant_id), K(sys_schema_version));
  } else {
    tenant_id_ = tenant_id;
    sys_schema_version_ = sys_schema_version;
  }
  return ret;
}

int ObBatchBroadcastSchemaArg::assign(const ObBatchBroadcastSchemaArg &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(tables_.assign(other.tables_))) {
    LOG_WARN("fail to assign tables", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    sys_schema_version_ = other.sys_schema_version_;
  }
  return ret;
}

void ObBatchBroadcastSchemaArg::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  sys_schema_version_ = common::OB_INVALID_VERSION;
  tables_.reset();
}

bool ObBatchBroadcastSchemaArg::is_valid() const
{
  return common::OB_INVALID_TENANT_ID != tenant_id_
         && sys_schema_version_ > 0
         && tables_.count() > 0;
}

uint64_t ObBatchBroadcastSchemaArg::get_tenant_id() const
{
  return tenant_id_;
}

int64_t ObBatchBroadcastSchemaArg::get_sys_schema_version() const
{
  return sys_schema_version_;
}

const common::ObIArray<share::schema::ObTableSchema> &ObBatchBroadcastSchemaArg::get_tables() const
{
  return tables_;
}
OB_SERIALIZE_MEMBER(ObBatchBroadcastSchemaArg, tenant_id_, sys_schema_version_, tables_);

ObBatchBroadcastSchemaResult::ObBatchBroadcastSchemaResult()
  : ret_(common::OB_ERROR)
{}

ObBatchBroadcastSchemaResult::~ObBatchBroadcastSchemaResult()
{}

int ObBatchBroadcastSchemaResult::assign(const ObBatchBroadcastSchemaResult &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    ret_ = other.ret_;
  }
  return ret;
}

void ObBatchBroadcastSchemaResult::reset()
{
  ret_ = common::OB_ERROR;
}

void ObBatchBroadcastSchemaResult::set_ret(int ret)
{
  ret_ = ret;
}

int ObBatchBroadcastSchemaResult::get_ret() const
{
  return ret_;
}

OB_SERIALIZE_MEMBER(ObBatchBroadcastSchemaResult, ret_);

ObRpcRemoteWriteDDLRedoLogArg::ObRpcRemoteWriteDDLRedoLogArg()
  : tenant_id_(OB_INVALID_ID), ls_id_(), redo_info_(), task_id_(0)
{}

int ObRpcRemoteWriteDDLRedoLogArg::init(const uint64_t tenant_id,
                                        const share::ObLSID &ls_id,
                                        const blocksstable::ObDDLMacroBlockRedoInfo &redo_info,
                                        const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_ID || task_id == 0 || !ls_id.is_valid() || !redo_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("args are not valid", K(ret), K(tenant_id), K(task_id), K(ls_id), K(redo_info));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    redo_info_ = redo_info;
    task_id_ = task_id;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRpcRemoteWriteDDLRedoLogArg, tenant_id_, ls_id_, redo_info_, task_id_);

ObRpcRemoteWriteDDLCommitLogArg::ObRpcRemoteWriteDDLCommitLogArg()
  : tenant_id_(OB_INVALID_ID), ls_id_(), table_key_(), start_scn_(SCN::min_scn()),
    table_id_(0), execution_id_(-1), ddl_task_id_(0)
{}

int ObRpcRemoteWriteDDLCommitLogArg::init(const uint64_t tenant_id,
                                          const share::ObLSID &ls_id,
                                          const storage::ObITable::TableKey &table_key,
                                          const SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_ID || !ls_id.is_valid() || !table_key.is_valid() || !start_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet id is not valid", K(ret), K(tenant_id), K(ls_id), K(table_key), K(start_scn));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    table_key_ = table_key;
    start_scn_ = start_scn;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRpcRemoteWriteDDLCommitLogArg, tenant_id_, ls_id_, table_key_, start_scn_,
                    table_id_, execution_id_, ddl_task_id_);


bool ObCheckLSCanOfflineArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && id_.is_valid()
         && (ls_is_tenant_dropping_status(current_ls_status_)
             || ls_is_dropping_status(current_ls_status_)
             || ls_is_empty_status(current_ls_status_)); //rpc from old version
}

void ObCheckLSCanOfflineArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  id_.reset();
  current_ls_status_ = share::ObLSStatus::OB_LS_EMPTY;
}

int ObCheckLSCanOfflineArg::assign(const ObCheckLSCanOfflineArg &arg)
{
  int ret = OB_SUCCESS;
  tenant_id_ = arg.tenant_id_;
  id_ = arg.id_;
  return ret;
}
int ObCheckLSCanOfflineArg::init(const uint64_t tenant_id,
    const share::ObLSID &id,
    const share::ObLSStatus &ls_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !id.is_valid()
                  || (!ls_is_tenant_dropping_status(ls_status) && ! ls_is_dropping_status(ls_status)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(id), K(ls_status));
  } else {
    tenant_id_ = tenant_id;
    id_ = id;
    current_ls_status_ = ls_status;
  }
  return ret;
}

DEF_TO_STRING(ObCheckLSCanOfflineArg)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id), K_(id), K_(current_ls_status));
  return pos;
}

OB_SERIALIZE_MEMBER(ObCheckLSCanOfflineArg, tenant_id_, id_, current_ls_status_);

bool ObRegisterTxDataArg::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID &&
         OB_NOT_NULL(tx_desc_) &&
         tx_desc_->is_valid() &&
         ls_id_.is_valid() &&
         type_ != ObTxDataSourceType::UNKNOWN;
}

int ObRegisterTxDataArg::init(const uint64_t tenant_id,
                              const ObTxDesc &tx_desc,
                              const ObLSID &ls_id,
                              const ObTxDataSourceType &type,
                              const ObString &buf,
                              const int64_t base_request_id,
                              const transaction::ObRegisterMdsFlag &register_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID || !tx_desc.is_valid() || !ls_id.is_valid()
                  || type == ObTxDataSourceType::UNKNOWN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tx_desc), K(ls_id), K(type));
  } else {
    tenant_id_ = tenant_id;
    tx_desc_ = const_cast<ObTxDesc *>(&tx_desc);
    ls_id_ = ls_id;
    type_ = type;
    buf_ = buf;
    request_id_ = base_request_id;
    register_flag_ = register_flag;
  }
  return ret;
}

void ObRegisterTxDataArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tx_desc_ = nullptr;
  ls_id_.reset();
  type_ = ObTxDataSourceType::UNKNOWN;
  buf_.reset();
  request_id_ = 0;
  register_flag_.reset();
  return;
}

void ObRegisterTxDataArg::inc_request_id(const int64_t base_request_id)
{
  if (-1 != base_request_id) {
    request_id_ = base_request_id + 1;
  } else {
    request_id_++;
  }
}

OB_DEF_SERIALIZE(ObRegisterTxDataArg)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(tenant_id_);
  OB_UNIS_ENCODE(*tx_desc_);
  LST_DO_CODE(OB_UNIS_ENCODE, ls_id_, type_, buf_, request_id_, register_flag_);
  return ret;
}
OB_DEF_DESERIALIZE(ObRegisterTxDataArg)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(tenant_id_);
  if (OB_SUCC(ret)) {
    ObTransService *tx_svc = MTL_WITH_CHECK_TENANT(ObTransService *, tenant_id_);
    if (OB_ISNULL(tx_svc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null tx service ptr", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(tx_svc->acquire_tx(buf, data_len, pos, tx_desc_))) {
      LOG_WARN("acquire tx by deserialize fail", K(data_len), K(pos), KR(ret));
    } else {
      LST_DO_CODE(OB_UNIS_DECODE, ls_id_, type_, buf_, request_id_, register_flag_);
      LOG_INFO("deserialize txDesc from session", KPC_(tx_desc), KPC(this));
    }
  }
  return ret;
}
OB_DEF_SERIALIZE_SIZE(ObRegisterTxDataArg)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(tenant_id_);
  OB_UNIS_ADD_LEN(*tx_desc_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, ls_id_, type_, buf_, request_id_, register_flag_);
  return len;
}

bool ObRegisterTxDataResult::is_valid() const
{
  return true;
}

int ObRegisterTxDataResult::init(const ObTxExecResult &tx_result)
{
  int ret = OB_SUCCESS;
  tx_result_ = tx_result;
  return ret;
}

void ObRegisterTxDataResult::reset()
{
  result_ = OB_SUCCESS;
  tx_result_.reset();
  return;
}

OB_SERIALIZE_MEMBER(ObRegisterTxDataResult, result_, tx_result_);

int ObRemoveSysLsArg::init(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else {
    server_ = server;
  }
  return ret;
}
int ObRemoveSysLsArg::assign(const ObRemoveSysLsArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    server_ = other.server_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRemoveSysLsArg, server_);

OB_SERIALIZE_MEMBER(ObQueryLSIsValidMemberRequest, tenant_id_, self_addr_, ls_array_);
OB_SERIALIZE_MEMBER(ObQueryLSIsValidMemberResponse, ret_value_, ls_array_, candidates_status_, ret_array_, gc_stat_array_);
OB_SERIALIZE_MEMBER(ObSwitchSchemaResult, ret_);

int ObTenantConfigArg::assign(const ObTenantConfigArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  config_str_ = other.config_str_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObTenantConfigArg, tenant_id_, config_str_);

OB_SERIALIZE_MEMBER(ObCheckBackupConnectivityArg, tenant_id_, backup_path_, check_path_);
ObCheckBackupConnectivityArg::ObCheckBackupConnectivityArg()
    : tenant_id_(OB_INVALID_TENANT_ID)
{
  backup_path_[0] = '\0';
  check_path_[0] = '\0';
}

int ObCheckBackupConnectivityArg::assign(const ObCheckBackupConnectivityArg &res)
{
  int ret = OB_SUCCESS;
  tenant_id_ = res.tenant_id_;
  if (OB_FAIL(databuff_printf(backup_path_, sizeof(backup_path_), "%s", res.backup_path_))) {
    LOG_WARN("fail to assign backup_dest", KR(ret));
  } else if (OB_FAIL(databuff_printf(check_path_, sizeof(check_path_), "%s", res.check_path_))) {
    LOG_WARN("fail to assign check_path", KR(ret));
  }
  return ret;
}

bool ObCheckBackupConnectivityArg::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_) && (0 != strlen(backup_path_)) && (0 != strlen(check_path_));
}

OB_SERIALIZE_MEMBER(ObReportBackupJobResultArg, tenant_id_, job_id_, result_);
ObReportBackupJobResultArg::ObReportBackupJobResultArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      job_id_(0),
      result_(OB_SUCCESS)
{
}

int ObReportBackupJobResultArg::assign(const ObReportBackupJobResultArg &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ret));
  } else {
    tenant_id_ = that.tenant_id_;
    job_id_ = that.job_id_;
    result_ = that.result_;
  }
  return ret;
}

int ObModifyPlanBaselineArg::assign(const ObModifyPlanBaselineArg &other)
{
  int ret = OB_SUCCESS;
  database_id_ = other.database_id_;
  sql_id_ = other.sql_id_;
  with_plan_hash_ = other.with_plan_hash_;
  plan_hash_ = other.plan_hash_;
  action_ = other.action_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObModifyPlanBaselineArg, tenant_id_, database_id_, sql_id_, with_plan_hash_, plan_hash_, action_);

int ObLoadPlanBaselineArg::assign(const ObLoadPlanBaselineArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  database_id_ = other.database_id_;
  sql_id_ = other.sql_id_;
  with_plan_hash_ = other.with_plan_hash_;
  plan_hash_value_ = other.plan_hash_value_;
  fixed_ = other.fixed_;
  enabled_ = other.enabled_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObLoadPlanBaselineArg, tenant_id_, database_id_, sql_id_, with_plan_hash_, plan_hash_value_, fixed_, enabled_);

int ObFlushOptStatArg::assign(const ObFlushOptStatArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  is_flush_col_usage_ = other.is_flush_col_usage_;
  is_flush_dml_stat_ = other.is_flush_dml_stat_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObFlushOptStatArg, tenant_id_, is_flush_col_usage_, is_flush_dml_stat_);

ObCancelDDLTaskArg::ObCancelDDLTaskArg()
  : task_id_()
{
}

ObCancelDDLTaskArg::ObCancelDDLTaskArg(const ObCurTraceId::TraceId &task_id)
  : task_id_(task_id)
{
}

int ObCancelDDLTaskArg::assign(const ObCancelDDLTaskArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(other));
  } else {
    task_id_ = other.task_id_;
  }
  return ret;
}

bool ObReportBackupJobResultArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_;
}

OB_SERIALIZE_MEMBER(ObCancelDDLTaskArg, task_id_);

int ObEstBlockArgElement::assign(const ObEstBlockArgElement &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  tablet_id_ = other.tablet_id_;
  ls_id_ = other.ls_id_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObEstBlockArgElement, tenant_id_, tablet_id_, ls_id_);


int ObEstBlockArg::assign(const ObEstBlockArg &other)
{
  return tablet_params_arg_.assign(other.tablet_params_arg_);
}

OB_SERIALIZE_MEMBER(ObEstBlockArg, tablet_params_arg_);

int ObEstBlockResElement::assign(const ObEstBlockResElement &other)
{
  int ret = OB_SUCCESS;
  macro_block_count_ = other.macro_block_count_;
  micro_block_count_ = other.micro_block_count_;
  sstable_row_count_ = other.sstable_row_count_;
  memtable_row_count_ = other.memtable_row_count_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObEstBlockResElement, macro_block_count_, micro_block_count_, sstable_row_count_, memtable_row_count_);

int ObEstBlockRes::assign(const ObEstBlockRes &other)
{
  return tablet_params_res_.assign(other.tablet_params_res_);
}

OB_SERIALIZE_MEMBER(ObEstBlockRes, tablet_params_res_);

OB_SERIALIZE_MEMBER(ObBatchGetTabletAutoincSeqArg, tenant_id_, ls_id_, src_tablet_ids_, dest_tablet_ids_);

int ObBatchGetTabletAutoincSeqArg::assign(const ObBatchGetTabletAutoincSeqArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  ls_id_ = other.ls_id_;
  if (OB_FAIL(src_tablet_ids_.assign(other.src_tablet_ids_))) {
    LOG_WARN("failed to assign src tablet ids", K(ret), K(other));
  } else if (OB_FAIL(dest_tablet_ids_.assign(other.dest_tablet_ids_))) {
    LOG_WARN("failed to assign dest tablet ids", K(ret), K(other));
  }
  return ret;
}

int ObBatchGetTabletAutoincSeqArg::init(const uint64_t tenant_id, const share::ObLSID &ls_id, const ObIArray<share::ObMigrateTabletAutoincSeqParam> &params)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  src_tablet_ids_.reset();
  dest_tablet_ids_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    const ObMigrateTabletAutoincSeqParam &param = params.at(i);
    if (OB_FAIL(src_tablet_ids_.push_back(param.src_tablet_id_))) {
      LOG_WARN("failed to push src tablet id", K(ret));
    } else if (OB_FAIL(dest_tablet_ids_.push_back(param.dest_tablet_id_))) {
      LOG_WARN("failed to push dest tablet id", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(*this));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBatchGetTabletAutoincSeqRes, autoinc_params_);

int ObBatchGetTabletAutoincSeqRes::assign(const ObBatchGetTabletAutoincSeqRes &other)
{
  return autoinc_params_.assign(other.autoinc_params_);
}

OB_SERIALIZE_MEMBER(ObBatchSetTabletAutoincSeqArg, tenant_id_, ls_id_, autoinc_params_);

int ObBatchSetTabletAutoincSeqArg::assign(const ObBatchSetTabletAutoincSeqArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  ls_id_ = other.ls_id_;
  if (OB_FAIL(autoinc_params_.assign(other.autoinc_params_))) {
    LOG_WARN("failed to assign autoinc params", K(ret), K(other));
  }
  return ret;
}

int ObBatchSetTabletAutoincSeqArg::init(const uint64_t tenant_id, const share::ObLSID &ls_id, const ObIArray<share::ObMigrateTabletAutoincSeqParam> &params)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  autoinc_params_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    const ObMigrateTabletAutoincSeqParam &param = params.at(i);
    if (OB_FAIL(autoinc_params_.push_back(param))) {
      LOG_WARN("failed to push dest tablet id", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(*this));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBatchSetTabletAutoincSeqRes, autoinc_params_);

int ObBatchSetTabletAutoincSeqRes::assign(const ObBatchSetTabletAutoincSeqRes &other)
{
  return autoinc_params_.assign(other.autoinc_params_);
}

OB_SERIALIZE_MEMBER(ObFetchLocationResult, servers_);

int ObFetchLocationResult::assign(const ObFetchLocationResult &other)
{
  return servers_.assign(other.servers_);
}

int ObFetchLocationResult::set_servers(
    const common::ObSArray<common::ObAddr> &servers)
{
  return servers_.assign(servers);
}

#ifdef OB_BUILD_ARBITRATION
OB_SERIALIZE_MEMBER(ObArbGCNotifyArg, epoch_, ls_ids_);

int ObArbGCNotifyArg::assign(const ObArbGCNotifyArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_ids_.assign(other.ls_ids_))) {
    LOG_WARN("assign ls_ids_ failed", K(ret), KPC(this), K(other));
  } else {
    epoch_ = other.epoch_;
  }
  return ret;
}

int ObArbGCNotifyArg::init(const arbserver::GCMsgEpoch &epoch,
                           const arbserver::TenantLSIDSArray &ls_ids)
{
  int ret = OB_SUCCESS;
  if (!epoch.is_valid() || !ls_ids.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(this), K(epoch), K(ls_ids));
  } else if (OB_FAIL(ls_ids_.assign(ls_ids))) {
    LOG_WARN("failed to assign ls_ids to ls_ids_", K(ret), KPC(this), K(ls_ids));
  } else {
    epoch_ = epoch;
    LOG_INFO("init ObArbGCNotifyArg success", KPC(this));
  }
  return ret;
}

bool ObArbGCNotifyArg::is_valid() const
{
  return epoch_.is_valid() && ls_ids_.is_valid();
}

const arbserver::GCMsgEpoch &ObArbGCNotifyArg::get_epoch() const
{
  return epoch_;
}

arbserver::TenantLSIDSArray &ObArbGCNotifyArg::get_ls_ids()
{
  return ls_ids_;
}

OB_SERIALIZE_MEMBER(ObArbGCNotifyResult, ret_);

void ObArbClusterOpArg::reset()
{
  type_ = MSG_TYPE::INVALID_MSG;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  cluster_name_.reset();
  epoch_.reset();
}

int ObArbClusterOpArg::assign(const ObArbClusterOpArg &other)
{
  type_ = other.type_;
  cluster_id_ = other.cluster_id_;
  cluster_name_ = other.cluster_name_;
  epoch_ = other.epoch_;
  return OB_SUCCESS;
}

int ObArbClusterOpArg::init(const int64_t cluster_id,
                                   const ObString &cluster_name,
                                   const arbserver::GCMsgEpoch &epoch,
                                   const bool is_add_arb)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_CLUSTER_ID == cluster_id || !epoch.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    type_ = (is_add_arb)? MSG_TYPE::CLUSTER_ADD_ARB: MSG_TYPE::CLUSTER_REMOVE_ARB;
    cluster_id_ = cluster_id;
    cluster_name_ = cluster_name;
    epoch_ = epoch;
  }
  return ret;
}

bool ObArbClusterOpArg::is_valid() const
{
  return type_ != MSG_TYPE::INVALID_MSG &&
      cluster_id_ != OB_INVALID_CLUSTER_ID &&
      epoch_.is_valid();
}

OB_SERIALIZE_MEMBER(ObArbClusterOpArg, type_, cluster_id_, cluster_name_, epoch_);

OB_SERIALIZE_MEMBER(ObArbClusterOpResult, ret_);
#endif

OB_SERIALIZE_MEMBER(ObSyncRewriteRuleArg, tenant_id_);

OB_SERIALIZE_MEMBER(ObSessInfoVerifyArg, sess_id_, proxy_sess_id_);

bool ObSessionInfoVeriRes::is_valid() const
{
  return true;
}

OB_DEF_SERIALIZE(ObSessionInfoVeriRes)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
          verify_info_buf_,
          need_verify_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSessionInfoVeriRes)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    ObString tmp_string;
    char *tmp_ptr = NULL;

    if (OB_FAIL(tmp_string.deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize nls_formats_", K(ret));
    } else if (OB_ISNULL(tmp_ptr = (char *)allocator_.alloc(tmp_string.length()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory!", K(ret));
    } else {
      MEMCPY(tmp_ptr, tmp_string.ptr(), tmp_string.length());
      verify_info_buf_.assign_ptr(tmp_ptr, tmp_string.length());
      tmp_string.reset();
    }
    if (OB_FAIL(ret)) {
      allocator_.free(tmp_ptr);
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
          need_verify_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSessionInfoVeriRes)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
          verify_info_buf_,
          need_verify_);
  }
  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

OB_SERIALIZE_MEMBER(ObGetLeaderLocationsArg, addr_);
OB_SERIALIZE_MEMBER(ObGetLeaderLocationsResult, addr_, leader_replicas_);

OB_SERIALIZE_MEMBER(ObInitTenantConfigArg, tenant_configs_);

int ObInitTenantConfigArg::assign(const ObInitTenantConfigArg &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(tenant_configs_.assign(other.tenant_configs_))) {
    LOG_WARN("fail to assign tenant configs", KR(ret), K(other));
      }
  return ret;
}

int ObRecompileAllViewsBatchArg::assign(const ObRecompileAllViewsBatchArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("failed to assign ddl arg", K(ret));
  } else if (OB_FAIL(view_ids_.assign(other.view_ids_))) {
    LOG_WARN("failed to assign view ids", K(ret));
  }
  return ret;
}

int ObInitTenantConfigArg::add_tenant_config(const ObTenantConfigArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_configs_.push_back(arg))) {
    LOG_WARN("fail to push back config configs", KR(ret), K(arg));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObInitTenantConfigRes, ret_);
int ObInitTenantConfigRes::assign(const ObInitTenantConfigRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    ret_ = other.ret_;
      }
  return ret;
}

OB_SERIALIZE_MEMBER((ObRecompileAllViewsBatchArg, ObDDLArg),
                    tenant_id_, view_ids_);

int ObTryAddDepInofsForSynonymBatchArg::assign(const ObTryAddDepInofsForSynonymBatchArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("failed to assign ddl arg", K(ret));
  } else if (OB_FAIL(synonym_ids_.assign(other.synonym_ids_))) {
    LOG_WARN("failed to assign view ids", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObRlsPolicyDDLArg, ObDDLArg), schema_, ddl_type_, option_bitset_);
int ObRlsPolicyDDLArg::assign(const ObRlsPolicyDDLArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(schema_.assign(other.schema_))) {
    LOG_WARN("fail to assign rls policy schema", KR(ret));
  } else {
    ddl_type_ = other.ddl_type_;
    option_bitset_ = other.option_bitset_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObRlsGroupDDLArg, ObDDLArg), schema_, ddl_type_);
int ObRlsGroupDDLArg::assign(const ObRlsGroupDDLArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(schema_.assign(other.schema_))) {
    LOG_WARN("fail to assign rls group schema", KR(ret));
  } else {
    ddl_type_ = other.ddl_type_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObRlsContextDDLArg, ObDDLArg), schema_, ddl_type_);
int ObRlsContextDDLArg::assign(const ObRlsContextDDLArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLArg::assign(other))) {
    LOG_WARN("fail to assign ddl arg", KR(ret));
  } else if (OB_FAIL(schema_.assign(other.schema_))) {
    LOG_WARN("fail to assign rls context schema", KR(ret));
  } else {
    ddl_type_ = other.ddl_type_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObStartTransferTaskArg, tenant_id_, task_id_, dest_ls_);

int ObStartTransferTaskArg::init(
    const uint64_t tenant_id,
    const ObTransferTaskID &task_id,
    const ObLSID &dest_ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || ! task_id.is_valid())
      || !dest_ls.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(task_id), K(dest_ls));
  } else {
    tenant_id_ = tenant_id;
    task_id_ = task_id;
    dest_ls_ = dest_ls;
  }
  return ret;
}

int ObStartTransferTaskArg::assign(const ObStartTransferTaskArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  task_id_ = other.task_id_;
  dest_ls_ = other.dest_ls_;
  return ret;
}


OB_SERIALIZE_MEMBER(ObFinishTransferTaskArg, tenant_id_, task_id_);

int ObFinishTransferTaskArg::init(const uint64_t tenant_id, const ObTransferTaskID &task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || ! task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(task_id));
  } else {
    tenant_id_ = tenant_id;
    task_id_ = task_id;
  }
  return ret;
}

int ObFinishTransferTaskArg::assign(const ObFinishTransferTaskArg &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  task_id_ = other.task_id_;
  return ret;
}

OB_SERIALIZE_MEMBER((ObTryAddDepInofsForSynonymBatchArg, ObDDLArg),
                    tenant_id_, synonym_ids_);

OB_SERIALIZE_MEMBER(ObGetServerResourceInfoArg, rs_addr_);

int ObGetServerResourceInfoArg::init(const common::ObAddr &rs_addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rs_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rs_addr", KR(ret), K(rs_addr));
  } else {
    rs_addr_ = rs_addr;
  }
  return ret;
}

int ObGetServerResourceInfoArg::assign(const ObGetServerResourceInfoArg &other)
{
  int ret = OB_SUCCESS;
  rs_addr_ = other.rs_addr_;
  return ret;
}

bool ObGetServerResourceInfoArg::is_valid() const
{
  return rs_addr_.is_valid();
}

void ObGetServerResourceInfoArg::reset()
{
  rs_addr_.reset();
}

OB_SERIALIZE_MEMBER(ObGetServerResourceInfoResult, server_, resource_info_);

int ObGetServerResourceInfoResult::init(
    const common::ObAddr &server,
    const share::ObServerResourceInfo &resource_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid() || !resource_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server or resource_info", KR(ret), K(server), K(resource_info));
  } else {
    server_ = server;
    resource_info_ = resource_info;
  }
  return ret;
}

int ObGetServerResourceInfoResult::assign(const ObGetServerResourceInfoResult &other)
{
  int ret = OB_SUCCESS;
  server_ = other.server_;
  resource_info_ = other.resource_info_;
  return ret;
}

bool ObGetServerResourceInfoResult::is_valid() const
{
  return server_.is_valid() && resource_info_.is_valid();
}

void ObGetServerResourceInfoResult::reset()
{
  server_.reset();
  resource_info_.reset();
}

OB_SERIALIZE_MEMBER(ObBroadcastConsensusVersionArg, tenant_id_, consensus_version_);
bool ObBroadcastConsensusVersionArg::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_VERSION != consensus_version_;
}

int ObBroadcastConsensusVersionArg::init(const uint64_t tenant_id, const int64_t consensus_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || OB_INVALID_VERSION == consensus_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(consensus_version));
  } else {
    tenant_id_ = tenant_id;
    consensus_version_ = consensus_version;
  }
  return ret;
}

int ObBroadcastConsensusVersionArg::assign(const ObBroadcastConsensusVersionArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    consensus_version_ = other.consensus_version_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObTTLResponseArg, tenant_id_, task_id_, server_addr_, task_status_, err_code_);
ObTTLResponseArg::ObTTLResponseArg()
    : tenant_id_(0),
      task_id_(OB_INVALID_ID),
      server_addr_(),
      task_status_(15),
      err_code_(OB_SUCCESS)
{}

int ObTTLResponseArg::assign(const ObTTLResponseArg &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    tenant_id_ = other.tenant_id_;
    task_id_ = other.task_id_;
    server_addr_ = other.server_addr_;
    task_status_ = other.task_status_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTTLRequestArg, cmd_code_, trigger_type_, task_id_, tenant_id_);

int ObTTLRequestArg::assign(const ObTTLRequestArg &other)
{
  int ret = OB_SUCCESS;

  cmd_code_ = other.cmd_code_;
  task_id_ = other.task_id_;
  tenant_id_ = other.tenant_id_;
  trigger_type_ = other.trigger_type_;

  return ret;
}

OB_SERIALIZE_MEMBER(ObRecoverTableArg, tenant_id_, tenant_name_, import_arg_, restore_tenant_arg_, action_);

ObRecoverTableArg::ObRecoverTableArg()
 : tenant_id_(OB_INVALID_TENANT_ID), tenant_name_(), import_arg_(), restore_tenant_arg_(), action_() {}

bool ObRecoverTableArg::is_valid() const
{
  bool ret = OB_INVALID_TENANT_ID != tenant_id_
          && (Action::CANCEL == action_ || !restore_tenant_arg_.restore_option_.empty());
  return ret;
}

int ObRecoverTableArg::assign(const ObRecoverTableArg &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid recover table arg", K(ret), K(that));
  } else if (OB_FAIL(import_arg_.assign(that.import_arg_))) {
    LOG_WARN("fail to assign", K(that.import_arg_));
  } else if (OB_FAIL(restore_tenant_arg_.assign(that.restore_tenant_arg_))) {
    LOG_WARN("failed to assign restore tenant arg", K(ret));
  } else {
    tenant_id_ = that.tenant_id_;
    tenant_name_ = that.tenant_name_;
    action_ = that.action_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBroadcastConsensusVersionRes, ret_);

int ObLoadBaselineRes::assign(const ObLoadBaselineRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    load_count_ = other.load_count_;
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObLoadBaselineRes, load_count_);

ObAdminUnlockMemberListOpArg::ObAdminUnlockMemberListOpArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    lock_id_(-1)
{
}

ObAdminUnlockMemberListOpArg::~ObAdminUnlockMemberListOpArg()
{
}

void ObAdminUnlockMemberListOpArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  lock_id_ = -1;
}

bool ObAdminUnlockMemberListOpArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && lock_id_ >= 0;
}

int ObAdminUnlockMemberListOpArg::set(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t lock_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || lock_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set unlock mmeber list arg get invalid argument", K(ret), K(tenant_id), K(ls_id), K(lock_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    lock_id_ = lock_id;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAdminUnlockMemberListOpArg, tenant_id_, ls_id_, lock_id_);

}//end namespace obrpc
}//end namepsace oceanbase
