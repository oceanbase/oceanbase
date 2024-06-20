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
#include "ob_storage_ha_diagnose_struct.h"

using namespace oceanbase;
using namespace share;
/**
 * ------------------------------ObStorageHAPerfDiagParams---------------------
 */
ObStorageHAPerfDiagParams::ObStorageHAPerfDiagParams()
  : dest_ls_id_(),
    task_id_(),
    task_type_(ObStorageHADiagTaskType::MAX_TYPE),
    item_type_(ObStorageHACostItemType::MAX_TYPE),
    name_(ObStorageHACostItemName::MAX_NAME),
    tablet_id_(),
    tablet_count_(0) {}

ObStorageHAPerfDiagParams::~ObStorageHAPerfDiagParams()
{
  reset();
}

void ObStorageHAPerfDiagParams::reset()
{
  dest_ls_id_.reset();
  task_id_.reset();
  task_type_ = ObStorageHADiagTaskType::MAX_TYPE;
  item_type_ = ObStorageHACostItemType::MAX_TYPE;
  name_ = ObStorageHACostItemName::MAX_NAME;
  tablet_id_.reset();
  tablet_count_ = 0;
}

bool ObStorageHAPerfDiagParams::is_valid() const
{
  return dest_ls_id_.is_valid()
      && task_type_ >= ObStorageHADiagTaskType::TRANSFER_START
      && task_type_ < ObStorageHADiagTaskType::MAX_TYPE
      && item_type_ >= ObStorageHACostItemType::ACCUM_COST_TYPE
      && item_type_ < ObStorageHACostItemType::MAX_TYPE
      && name_ >= ObStorageHACostItemName::TRANSFER_START_BEGIN
      && name_ < ObStorageHACostItemName::MAX_NAME
      && tablet_count_ > 0;
}

int ObStorageHAPerfDiagParams::assign(const ObStorageHAPerfDiagParams &params)
{
  int ret = OB_SUCCESS;
  if (!params.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params is invalid", K(ret), K(params));
  } else if (this != &params) {
    dest_ls_id_ = params.dest_ls_id_;
    task_id_ = params.task_id_;
    item_type_ = params.item_type_;
    name_ = params.name_;
    tablet_id_ = params.tablet_id_;
    tablet_count_ = params.tablet_count_;
  }
  return ret;
}

/**
 * ------------------------------ObStorageHADiagTaskKey---------------------
 */
ObStorageHADiagTaskKey::ObStorageHADiagTaskKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    task_id_(0),
    module_(ObStorageHADiagModule::MAX_MODULE),
    type_(ObStorageHADiagTaskType::MAX_TYPE),
    retry_id_(0),
    diag_type_(ObStorageHADiagType::MAX_TYPE),
    tablet_id_()
{
}

void ObStorageHADiagTaskKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  task_id_ = 0;
  module_ = ObStorageHADiagModule::MAX_MODULE;
  type_ = ObStorageHADiagTaskType::MAX_TYPE;
  retry_id_ = 0;
  diag_type_ = ObStorageHADiagType::MAX_TYPE;
  tablet_id_.reset();
}

bool ObStorageHADiagTaskKey::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID
      && task_id_ >= 0
      && module_ < ObStorageHADiagModule::MAX_MODULE
      && module_ >= ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE
      && type_ < ObStorageHADiagTaskType::MAX_TYPE
      && type_ >= ObStorageHADiagTaskType::TRANSFER_START
      && retry_id_ >= 0
      && diag_type_ < ObStorageHADiagType::MAX_TYPE
      && diag_type_ >= ObStorageHADiagType::ERROR_DIAGNOSE;
}

bool ObStorageHADiagTaskKey::operator==(const ObStorageHADiagTaskKey &key) const
{
  bool ret = (tenant_id_ == key.tenant_id_
      && task_id_ == key.task_id_
      && module_ == key.module_
      && type_ == key.type_
      && diag_type_ == key.diag_type_);
  if (ObStorageHADiagTaskType::TRANSFER_BACKFILLED == type_ && type_ == key.type_) {
    ret = ret && (tablet_id_ == key.tablet_id_);
  }

  if (ObStorageHADiagType::ERROR_DIAGNOSE == key.diag_type_ && key.diag_type_ == diag_type_) {
    ret = ret && (retry_id_ == key.retry_id_);
  }
  return ret;
}

bool ObStorageHADiagTaskKey::operator<(const ObStorageHADiagTaskKey &key) const
{
  return tenant_id_ < key.tenant_id_;
}

int ObStorageHADiagTaskKey::assign(const ObStorageHADiagTaskKey &key)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (this != &key) {
    tenant_id_ = key.tenant_id_;
    task_id_ = key.task_id_;
    module_ = key.module_;
    type_ = key.type_;
    retry_id_ = key.retry_id_;
    diag_type_ = key.diag_type_;
    tablet_id_ = key.tablet_id_;
  }
  return ret;
}

uint64_t ObStorageHADiagTaskKey::hash() const
{
  uint64_t hash_val = 0;

  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&task_id_, sizeof(task_id_), hash_val);
  hash_val = murmurhash(&module_, sizeof(module_), hash_val);
  hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  hash_val = murmurhash(&retry_id_, sizeof(retry_id_), hash_val);
  hash_val = murmurhash(&diag_type_, sizeof(diag_type_), hash_val);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  return hash_val;
}

int ObStorageHADiagTaskKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

/**
 * ------------------------------ObStorageHADiagInfo---------------------
 */
ObStorageHADiagInfo::ObStorageHADiagInfo()
  : task_id_(0),
    ls_id_(),
    module_(ObStorageHADiagModule::MAX_MODULE),
    type_(ObStorageHADiagTaskType::MAX_TYPE),
    retry_id_(0),
    result_code_(0),
    timestamp_(0),
    result_msg_(ObStorageHACostItemName::MAX_NAME),
    task_(nullptr),
    tablet_id_()
{
}

int ObStorageHADiagInfo::assign(const ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is invalid", K(ret), K(info));
  } else {
    task_id_ = info.task_id_;
    ls_id_ = info.ls_id_;
    module_ = info.module_;
    type_ = info.type_ ;
    retry_id_ = info.retry_id_;
    result_code_ = info.result_code_;
    timestamp_ = info.timestamp_;
    result_msg_ = info.result_msg_;
    tablet_id_ = info.tablet_id_;
  }
  return ret;
}

void ObStorageHADiagInfo::reset()
{
  task_id_ = 0;
  ls_id_.reset();
  module_ = ObStorageHADiagModule::MAX_MODULE;
  type_ = ObStorageHADiagTaskType::MAX_TYPE;
  retry_id_ = 0;
  result_code_ = 0;
  timestamp_ = 0;
  result_msg_ = ObStorageHACostItemName::MAX_NAME;
  task_ = nullptr;
  tablet_id_.reset();
}

bool ObStorageHADiagInfo::is_valid() const
{
  return task_id_ > 0
      && ls_id_.is_valid()
      && module_ < ObStorageHADiagModule::MAX_MODULE
      && module_ >= ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE
      && type_ < ObStorageHADiagTaskType::MAX_TYPE
      && type_ >= ObStorageHADiagTaskType::TRANSFER_START
      && retry_id_ >= 0
      && result_code_ <= 0
      && timestamp_ >= 0;
}

const char *ObStorageHADiagInfo::ObStorageDiagModuleStr[static_cast<int64_t>(ObStorageHADiagModule::MAX_MODULE)] = {
    "TRANSFER_ERROR_DIAGNOSE",
    "TRANSFER_PERF_DIAGNOSE"
};

const char *ObStorageHADiagInfo::ObStorageDiagTaskTypeStr[static_cast<int64_t>(ObStorageHADiagTaskType::MAX_TYPE)] = {
    "TRANSFER_START",
    "TRANSFER_DOING",
    "TRANSFER_ABORT",
    "TRANSFER_START_IN",
    "TRANSFER_START_OUT",
    "TRANSFER_FINISH_IN",
    "TRANSFER_FINISH_OUT",
    "TRANSFER_BACKFILLED"
};

const char * ObStorageHADiagInfo::ObTransferErrorDiagMsg[static_cast<int>(share::ObStorageHACostItemName::MAX_NAME)] = {
  "TRANSFER_START_BEGIN_ERROR",
  "LOCK_MEMBER_LIST_ERROR",
  "PRECHECK_LS_REPALY_SCN_ERROR",
  "CHECK_START_STATUS_TRANSFER_TABLETS_ERROR",
  "CHECK_SRC_LS_HAS_ACTIVE_TRANS_ERROR",
  "UPDATE_ALL_TABLET_TO_LS_ERROR",
  "LOCK_TABLET_ON_DEST_LS_FOR_TABLE_LOCK_ERROR",
  "BLOCK_AND_KILL_TX_ERROR",
  "REGISTER_TRANSFER_START_OUT_ERROR",
  "DEST_LS_GET_START_SCN_ERROR",
  "CHECK_SRC_LS_HAS_ACTIVE_TRANS_LATER_ERROR",
  "UNBLOCK_TX_ERROR",
  "WAIT_SRC_LS_REPLAY_TO_START_SCN_ERROR",
  "SRC_LS_GET_TABLET_META_ERROR",
  "REGISTER_TRANSFER_START_IN_ERROR",
  "START_TRANS_COMMIT_ERROR",
  "TRANSFER_START_END_ERROR",
  "TRANSFER_FINISH_BEGIN_ERROR",
  "UNLOCK_SRC_AND_DEST_LS_MEMBER_LIST_ERROR",
  "CHECK_LS_LOGICAL_TABLE_REPLACED_ERROR",
  "LOCK_LS_MEMBER_LIST_IN_DOING_ERROR",
  "CHECK_LS_LOGICAL_TABLE_REPLACED_LATER_ERROR",
  "REGISTER_TRANSFER_FINISH_IN_ERROR",
  "WAIT_TRANSFER_TABLET_STATUS_NORMAL_ERROR",
  "WAIT_ALL_LS_REPLICA_REPLAY_FINISH_SCN_ERROR",
  "REGISTER_TRANSFER_OUT_ERROR",
  "UNLOCK_TABLET_FOR_LOCK_ERROR",
  "UNLOCK_LS_MEMBER_LIST_ERROR",
  "FINISH_TRANS_COMMIT_ERROR",
  "TRANSFER_FINISH_END_ERROR",
  "TRANSFER_ABORT_BEGIN_ERROR",
  "UNLOCK_MEMBER_LIST_IN_ABORT_ERROR",
  "ABORT_TRANS_COMMIT_ERROR",
  "TRANSFER_ABORT_END_ERROR",
  "TRANSFER_BACKFILLED_TABLE_BEGIN_ERROR",
  "TX_BACKFILL_ERROR",
  "TABLET_REPLACE_ERROR",
  "REPLACE_TIMESTAMP_ERROR",
  "TRANSFER_BACKFILLED_END_ERROR",
  "START_TRANSFER_OUT_FIRST_REPALY_LOG_TIMESTAMP_ERROR",
  "START_TRANSFER_OUT_LOG_SCN_ERROR",
  "START_TRANSFER_OUT_TX_ID_ERROR",
  "START_TRANSFER_OUT_REPLAY_COST_ERROR",
  "START_TRANSFER_IN_FIRST_REPALY_LOG_TIMESTAMP_ERROR",
  "START_TRANSFER_IN_LOG_SCN_ERROR",
  "START_TRANSFER_IN_TX_ID_ERROR",
  "START_TRANSFER_IN_REPLAY_COST_ERROR",
  "START_CHECK_LS_EXIST_TS_ERROR",
  "START_CHECK_LS_REPLAY_POINT_TS_ERROR",
  "FINISH_TRANSFER_OUT_FIRST_REPALY_LOG_TIMESTAMP_ERROR",
  "FINISH_TRANSFER_OUT_LOG_SCN_ERROR",
  "FINISH_TRANSFER_OUT_TX_ID_ERROR",
  "FINISH_TRANSFER_OUT_REPLAY_COST_ERROR",
  "FINISH_TRANSFER_IN_FIRST_REPALY_LOG_TIMESTAMP_ERROR",
  "FINISH_TRANSFER_IN_LOG_SCN_ERROR",
  "FINISH_TRANSFER_IN_TX_ID_ERROR",
  "FINISH_TRANSFER_IN_REPLAY_COST_ERROR",
  "UNLOCK_MEMBER_LIST_IN_START_ERROR",
  "ON_REGISTER_SUCCESS_ERROR",
  "ON_REPLAY_SUCCESS_ERROR",
  "PREPARE_MERGE_CTX_ERROR",
  "PREPARE_INDEX_TREE_ERROR",
  "DO_BACKFILL_TX_ERROR",
  "TRANSFER_REPLACE_BEGIN_ERROR",
  "TRANSFER_REPLACE_END_ERROR",
  "TRANSFER_BACKFILL_START_ERROR",
  "STOP_LS_SCHEDULE_MEMDIUM_ERROR",
};

const char * ObStorageHADiagInfo::get_transfer_error_diagnose_msg() const
{
  const char *str = "";
  if (result_msg_ < ObStorageHACostItemName::TRANSFER_START_BEGIN || result_msg_ >= ObStorageHACostItemName::MAX_NAME) {
    str = "Unstatistical errors";
  } else {
    str = ObTransferErrorDiagMsg[static_cast<int>(result_msg_)];
  }
  return str;
}

const char * ObStorageHADiagInfo::get_module_str() const
{
  const char *str = "";
  if (module_ >= ObStorageHADiagModule::MAX_MODULE || module_ < ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE) {
    str = "invalid_module";
  } else {
    str = ObStorageDiagModuleStr[static_cast<int64_t>(module_)];
  }
  return str;
}

const char * ObStorageHADiagInfo::get_type_str() const
{
  const char *str = "";
  if (type_ >= ObStorageHADiagTaskType::MAX_TYPE || type_ < ObStorageHADiagTaskType::TRANSFER_START) {
    str = "invalid_type";
  } else {
    str = ObStorageDiagTaskTypeStr[static_cast<int64_t>(type_)];
  }
  return str;
}
/**
 * ------------------------------ObStorageHADiagTask---------------------
 */
ObStorageHADiagTask::ObStorageHADiagTask()
  : key_(),
    val_(nullptr)
{
}

ObStorageHADiagTask::~ObStorageHADiagTask()
{
  reset();
}

void ObStorageHADiagTask::reset()
{
  key_.reset();
  val_ = nullptr;
}

bool ObStorageHADiagTask::is_valid() const
{
  return key_.is_valid() && val_ != nullptr;
}
/**
 * ------------------------------ObTransferErrorDiagInfo---------------------
 */
ObTransferErrorDiagInfo::ObTransferErrorDiagInfo()
  : ObStorageHADiagInfo(),
    trace_id_(),
    thread_id_(0)
{
}

int ObTransferErrorDiagInfo::get_info(char *info, const int64_t size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else {
    const int64_t MAX_TRACE_ID_LENGTH = 64;
    char trace_id[MAX_TRACE_ID_LENGTH] = { 0 };
    int64_t pos = 0;
    memset(info, 0, size);
    if (0 == trace_id_.to_string(trace_id, sizeof(trace_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to print trace id.", K(ret), K(trace_id_));
    } else if (OB_FAIL(databuff_printf(info, size, pos, "%s%s%s%ld", "trace_id:", trace_id, "|thread_id:", thread_id_))) {
      LOG_WARN("failed to print info.", K(ret), K(size), K(trace_id_), K(pos), K(thread_id_));
    }
  }
  return ret;
}

int ObTransferErrorDiagInfo::get_task_id(char *info, const int64_t size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else {
    int64_t pos = 0;
    memset(info, 0, size);
    if (OB_FAIL(databuff_printf(info, size, pos, "%ld", task_id_))) {
      LOG_WARN("failed to print task_id.", K(ret), K(size), K(pos), K(task_id_));
    }
  }
  return ret;
}

bool ObTransferErrorDiagInfo::is_valid() const
{
  return ObStorageHADiagInfo::is_valid()
      && trace_id_.is_valid()
      && thread_id_ > 0;
}

void ObTransferErrorDiagInfo::reset()
{
  ObStorageHADiagInfo::reset();
  trace_id_.reset();
  thread_id_ = 0;
}

int ObTransferErrorDiagInfo::update(const ObStorageHADiagInfo &info)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("NOT SUPPORTED UPDATE", K(ret));
  return ret;
}

int ObTransferErrorDiagInfo::assign(const ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is invalid", K(ret), K(info));
  } else if (ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE != info.module_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info module type is unexpected", K(ret), K(info.module_));
  } else if (OB_FAIL(ObStorageHADiagInfo::assign(info))) {
    LOG_WARN("failed to assign info.", K(ret), K(info));
  } else {
    trace_id_ = static_cast<const ObTransferErrorDiagInfo &>(info).trace_id_;
    thread_id_ = static_cast<const ObTransferErrorDiagInfo &>(info).thread_id_;
  }
  return ret;
}

int ObTransferErrorDiagInfo::deep_copy(ObIAllocator &allocator, ObStorageHADiagInfo *&out_info) const
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  out_info = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTransferErrorDiagInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObTransferErrorDiagInfo *info = nullptr;
    if (OB_ISNULL(info = (new (buf) ObTransferErrorDiagInfo()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("info is nullptr", K(ret));
    } else {
      buf = nullptr;
      if (OB_FAIL(info->assign(*this))) {
        LOG_WARN("failed to assign info", K(ret), KPC(this));
      } else {
        out_info = info;
        info = nullptr;
      }
      if (OB_NOT_NULL(info)) {
        info->~ObTransferErrorDiagInfo();
        allocator.free(info);
        info = nullptr;
      }
    }
    if (nullptr != buf) {
      allocator.free(buf);
      buf = nullptr;
    }

  }
  return ret;
}
/**
 * ------------------------------ObIStorageHACostItem---------------------
 */
ObIStorageHACostItem::ObIStorageHACostItem()
  : name_(ObStorageHACostItemName::MAX_NAME),
    type_(ObStorageHACostItemType::MAX_TYPE),
    retry_id_(0)
{
}

bool ObIStorageHACostItem::is_valid() const
{
  return name_ >= ObStorageHACostItemName::TRANSFER_START_BEGIN
         && name_ < ObStorageHACostItemName::MAX_NAME
         && type_ >= ObStorageHACostItemType::ACCUM_COST_TYPE
         && type_ < ObStorageHACostItemType::MAX_TYPE
         && retry_id_ >= 0;
}

void ObIStorageHACostItem::reset()
{
  name_ = ObStorageHACostItemName::MAX_NAME;
  type_ = ObStorageHACostItemType::MAX_TYPE;
  retry_id_ = 0;
}

uint32_t ObIStorageHACostItem::get_size() const
{
  return sizeof(ObStorageHACostItemName) + sizeof(ObStorageHACostItemType) + sizeof(int64_t);
}

int ObIStorageHACostItem::assign(const ObIStorageHACostItem &item)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else {
    name_ = item.name_;
    type_ = item.type_;
    retry_id_ = item.retry_id_;
  }
  return ret;
}

const char *ObIStorageHACostItem::ObStorageHACostItemNameStr[static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME)] = {
  "TRANSFER_START_BEGIN",
  "LOCK_MEMBER_LIST",
  "PRECHECK_LS_REPALY_SCN",
  "CHECK_START_STATUS_TRANSFER_TABLETS",
  "CHECK_SRC_LS_HAS_ACTIVE_TRANS",
  "UPDATE_ALL_TABLET_TO_LS",
  "LOCK_TABLET_ON_DEST_LS_FOR_TABLE_LOCK",
  "BLOCK_AND_KILL_TX",
  "REGISTER_TRANSFER_START_OUT",
  "DEST_LS_GET_START_SCN",
  "CHECK_SRC_LS_HAS_ACTIVE_TRANS_LATER",
  "UNBLOCK_TX",
  "WAIT_SRC_LS_REPLAY_TO_START_SCN",
  "SRC_LS_GET_TABLET_META",
  "REGISTER_TRANSFER_START_IN",
  "START_TRANS_COMMIT",
  "TRANSFER_START_END",
  "TRANSFER_FINISH_BEGIN",
  "UNLOCK_SRC_AND_DEST_LS_MEMBER_LIST",
  "CHECK_LS_LOGICAL_TABLE_REPLACED",
  "LOCK_LS_MEMBER_LIST_IN_DOING",
  "CHECK_LS_LOGICAL_TABLE_REPLACED_LATER",
  "REGISTER_TRANSFER_FINISH_IN",
  "WAIT_TRANSFER_TABLET_STATUS_NORMAL",
  "WAIT_ALL_LS_REPLICA_REPLAY_FINISH_SCN",
  "REGISTER_TRANSFER_OUT",
  "UNLOCK_TABLET_FOR_LOCK",
  "UNLOCK_LS_MEMBER_LIST",
  "FINISH_TRANS_COMMIT",
  "TRANSFER_FINISH_END",
  "TRANSFER_ABORT_BEGIN",
  "UNLOCK_MEMBER_LIST_IN_ABORT",
  "ABORT_TRANS_COMMIT",
  "TRANSFER_ABORT_END",
  "TRANSFER_BACKFILLED_TABLE_BEGIN",
  "TX_BACKFILL",
  "TABLET_REPLACE",
  "REPLACE_TIMESTAMP",
  "TRANSFER_BACKFILLED_END",
  "START_TRANSFER_OUT_FIRST_REPALY_LOG_TIMESTAMP",
  "START_TRANSFER_OUT_LOG_SCN",
  "START_TRANSFER_OUT_TX_ID",
  "START_TRANSFER_OUT_REPLAY_COST",
  "START_TRANSFER_IN_FIRST_REPALY_LOG_TIMESTAMP",
  "START_TRANSFER_IN_LOG_SCN",
  "START_TRANSFER_IN_TX_ID",
  "START_TRANSFER_IN_REPLAY_COST",
  "START_CHECK_LS_EXIST_TS",
  "START_CHECK_LS_REPLAY_POINT_TS",
  "FINISH_TRANSFER_OUT_FIRST_REPALY_LOG_TIMESTAMP",
  "FINISH_TRANSFER_OUT_LOG_SCN",
  "FINISH_TRANSFER_OUT_TX_ID",
  "FINISH_TRANSFER_OUT_REPLAY_COST",
  "FINISH_TRANSFER_IN_FIRST_REPALY_LOG_TIMESTAMP",
  "FINISH_TRANSFER_IN_LOG_SCN",
  "FINISH_TRANSFER_IN_TX_ID",
  "FINISH_TRANSFER_IN_REPLAY_COST",
  "UNLOCK_MEMBER_LIST_IN_START",
  "ON_REGISTER_SUCCESS",
  "ON_REPLAY_SUCCESS",
  "PREPARE_MERGE_CTX",
  "PREPARE_INDEX_TREE",
  "DO_BACKFILL_TX",
  "TRANSFER_REPLACE_BEGIN",
  "TRANSFER_REPLACE_END",
  "TRANSFER_BACKFILL_START",
  "STOP_LS_SCHEDULE_MEMDIUM",
};

const char *ObIStorageHACostItem::ObStorageHACostItemTypeStr[static_cast<int64_t>(ObStorageHACostItemType::MAX_TYPE)] = {
  "ACCUM_COST_TYPE",
  "FLUENT_TIMESTAMP_TYPE",
  "CRUCIAL_TIMESTAMP",
};

const char *ObIStorageHACostItem::get_item_name() const
{
  const char *str = "";
  if (name_ < ObStorageHACostItemName::TRANSFER_START_BEGIN || name_ >= ObStorageHACostItemName::MAX_NAME) {
    str = "invalid_name";
  } else {
    str = ObStorageHACostItemNameStr[static_cast<int64_t>(name_)];
  }
  return str;
}

const char *ObIStorageHACostItem::get_item_type() const
{
  const char *str = "";
  if (type_ < ObStorageHACostItemType::ACCUM_COST_TYPE || type_ >= ObStorageHACostItemType::MAX_TYPE) {
    str = "invalid_type";
  } else {
    str = ObStorageHACostItemTypeStr[static_cast<int64_t>(type_)];
  }
  return str;
}
/**
 * ------------------------------ObStorageHACostAccumItem---------------------
 */
ObStorageHACostAccumItem::ObStorageHACostAccumItem()
  : ObIStorageHACostItem(),
    cost_time_(0) {}

int ObStorageHACostAccumItem::update(const ObIStorageHACostItem &item)
{
  int ret = OB_SUCCESS;
  if (ObStorageHACostItemType::ACCUM_COST_TYPE != item.type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("crucial cost accum type can not update", K(ret), K(item.type_));
  } else {
    cost_time_ += static_cast<const ObStorageHACostAccumItem &>(item).cost_time_;
    retry_id_ = static_cast<const ObStorageHACostAccumItem &>(item).retry_id_;
  }
  return ret;
}

int ObStorageHACostAccumItem::get_str(char *str, const int64_t size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size), K(pos));
  } else if (OB_FAIL(databuff_printf(str, size, pos, "%s:%ldus,retry_id:%ld|", get_item_name(), cost_time_, retry_id_))) {
    LOG_WARN("failed to print info.", K(ret), K(size), K(name_), K(pos), K(cost_time_));
  }
  return ret;
}

int ObStorageHACostAccumItem::deep_copy(ObIAllocator &allocator, ObIStorageHACostItem *&out_item) const
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  out_item = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStorageHACostAccumItem)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObStorageHACostAccumItem *item = nullptr;
    if (OB_ISNULL(item = (new (buf) ObStorageHACostAccumItem()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item is nullptr", K(ret));
    } else {
      buf = nullptr;
      if (OB_FAIL(item->assign(*this))) {
        LOG_WARN("failed to assign info", K(ret), KPC(this));
      } else {
        out_item = item;
        item = nullptr;
      }
      if (OB_NOT_NULL(item)) {
        item->~ObStorageHACostAccumItem();
        allocator.free(item);
        item = nullptr;
      }
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

bool ObStorageHACostAccumItem::is_valid() const
{
  return ObIStorageHACostItem::is_valid()
         && cost_time_ >= 0;
}

void ObStorageHACostAccumItem::reset()
{
  ObIStorageHACostItem::reset();
  cost_time_ = 0;
}

int ObStorageHACostAccumItem::assign(const ObIStorageHACostItem &item)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (ObStorageHACostItemType::ACCUM_COST_TYPE != item.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected item type", K(ret), K(item.type_));
  } else if (OB_FAIL(ObIStorageHACostItem::assign(item))) {
    LOG_WARN("fail to assign item", K(ret), K(item));
  } else {
    cost_time_ = static_cast<const ObStorageHACostAccumItem &>(item).cost_time_;
  }
  return ret;
}

uint32_t ObStorageHACostAccumItem::get_size() const
{
  return ObIStorageHACostItem::get_size() + sizeof(uint64_t);
}
/**
 * ------------------------------ObStorageHATimestampItem---------------------
 */
ObStorageHATimestampItem::ObStorageHATimestampItem()
  : ObIStorageHACostItem(),
    timestamp_(0) {}

int ObStorageHATimestampItem::get_str(char *str, const int64_t size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size), K(pos));
  } else if (OB_FAIL(databuff_printf(str, size, pos, "%s:%ld,retry_id:%ld|", get_item_name(), timestamp_, retry_id_))) {
    LOG_WARN("failed to print info.", K(ret), K(size), K(name_), K(pos), K(timestamp_));
  }
  return ret;
}

bool ObStorageHATimestampItem::is_valid() const
{
  return ObIStorageHACostItem::is_valid()
         && timestamp_ >= 0;
}

void ObStorageHATimestampItem::reset()
{
  ObIStorageHACostItem::reset();
  timestamp_ = 0;
}

int ObStorageHATimestampItem::assign(const ObIStorageHACostItem &item)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (ObStorageHACostItemType::FLUENT_TIMESTAMP_TYPE != item.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected item type", K(ret), K(item.type_));
  } else if (OB_FAIL(ObIStorageHACostItem::assign(item))) {
    LOG_WARN("fail to assign item", K(ret), K(item));
  } else {
    timestamp_ = static_cast<const ObStorageHATimestampItem &>(item).timestamp_;
  }
  return ret;
}

int ObStorageHATimestampItem::deep_copy(ObIAllocator &allocator, ObIStorageHACostItem *&out_item) const
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  out_item = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStorageHATimestampItem)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObStorageHATimestampItem *item = nullptr;
    if (OB_ISNULL(item = (new (buf) ObStorageHATimestampItem()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item is nullptr", K(ret));
    } else {
      buf = nullptr;
      if (OB_FAIL(item->assign(*this))) {
        LOG_WARN("failed to assign info", K(ret), KPC(this));
      } else {
        out_item = item;
        item = nullptr;
      }
      if (OB_NOT_NULL(item)) {
        item->~ObStorageHATimestampItem();
        allocator.free(item);
        item = nullptr;
      }
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObStorageHATimestampItem::update(const ObIStorageHACostItem &item)
{
  int ret = OB_SUCCESS;
  if (ObStorageHACostItemType::ACCUM_COST_TYPE == item.type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("crucial cost accum type can not update", K(ret), K(item.type_));
  } else {
    // do nothing, FLUENT_TIMESTAMP_TYPE and CRUCIAL_TIMESTAMP only
    // record the time of the first generation and do not update with retries.
  }
  return ret;
}

uint32_t ObStorageHATimestampItem::get_size() const
{
  return ObIStorageHACostItem::get_size() + sizeof(uint64_t);
}
/**
 * ------------------------------ObTransferPerfDiagInfo---------------------
 */
ObTransferPerfDiagInfo::ObTransferPerfDiagInfo()
  : ObStorageHADiagInfo(),
    end_timestamp_(0),
    tablet_count_(0),
    items_(),
    item_map_(),
    item_size_(0),
    is_inited_(false),
    allocator_(nullptr)
{
}

ObTransferPerfDiagInfo::~ObTransferPerfDiagInfo()
{
  destroy();
}

int ObTransferPerfDiagInfo::init(ObIAllocator *allocator, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "PerfDiagItem");
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(item_map_.create(PERF_DIAG_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create item map", K(ret));
  } else {
    allocator_ = allocator;
    is_inited_ = true;
  }
  return ret;
}

bool ObTransferPerfDiagInfo::is_valid() const
{
  return is_inited_ && end_timestamp_ > 0 && tablet_count_ > 0
      && !items_.is_empty() && !item_map_.empty();
}

void ObTransferPerfDiagInfo::clear()
{
  int ret = OB_SUCCESS;
  ObIStorageHACostItem *item = nullptr;
  if (!is_inited_) {
    if (items_.get_size() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("items_ should is null", K(ret), K(items_.get_size()));
    }
  } else {
    while (OB_SUCC(ret) && items_.get_size() > 0) {
      if (OB_ISNULL(item = items_.remove_first())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to remove first item", K(ret));
      } else {
        item->~ObIStorageHACostItem();
        allocator_->free(item);
        item = nullptr;
      }
    }
  }
  ObStorageHADiagInfo::reset();
  end_timestamp_ = 0;
  tablet_count_ = 0;
  item_size_ = 0;
  items_.clear();
}

void ObTransferPerfDiagInfo::destroy()
{
  clear();
  allocator_ = nullptr;
  is_inited_ = false;
  item_map_.destroy();
}

void ObTransferPerfDiagInfo::reuse()
{
  clear();
  item_map_.reuse();
}

int ObTransferPerfDiagInfo::add_item(const ObIStorageHACostItem &item)
{
  int ret = OB_SUCCESS;
  ObIStorageHACostItem *copy_item = nullptr;
  bool add_list = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("item is invalid", K(ret), K(item));
  } else if (item_size_ + item.get_size() > ITEM_MAX_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("items size overflow", K(ret), K(item_size_), K(item.get_size()));
  } else if (OB_FAIL(item.deep_copy(*allocator_, copy_item))) {
    LOG_WARN("fail to deep copy item", K(ret), K(item));
  } else if (!items_.add_last(copy_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add item", K(ret));
  } else {
    add_list = true;
    int is_overwrite = 0; // do not overwrite
    if (OB_FAIL(item_map_.set_refactored(static_cast<int>(item.name_), copy_item, is_overwrite))) {
      if (OB_HASH_EXIST == ret) {
        // overwrite ret
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item exist", K(ret));
      } else {
        LOG_WARN("fail to add item to set", K(ret));
      }
    } else {
      item_size_ += item.get_size();
    }
  }
  if (OB_FAIL(ret) && NULL != copy_item) {
    if (add_list) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(items_.remove(copy_item))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to remove item", K(tmp_ret), K(*copy_item));
      }
    }
    copy_item->~ObIStorageHACostItem();
    allocator_->free(copy_item);
    copy_item = NULL;
  }
  return ret;
}

int ObTransferPerfDiagInfo::find_item_(const ObStorageHACostItemName &name, bool &found, ObIStorageHACostItem *&item)
{
  int ret = OB_SUCCESS;
  item = nullptr;
  found = false;
  if (name >= ObStorageHACostItemName::MAX_NAME || name < ObStorageHACostItemName::TRANSFER_START_BEGIN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(name));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(item_map_.get_refactored(static_cast<int>(name), item))) {
    if (OB_HASH_NOT_EXIST == ret) {
      //overwrite ret
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get retry id from map", K(ret), K(name));
    }
  } else {
    found = true;
  }
  return ret;
}

int ObTransferPerfDiagInfo::update_item_list_(const ObTransferPerfDiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (info.items_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info.items_));
  } else {
    ObIStorageHACostItem *old_item = nullptr;
    bool found = false;
    DLIST_FOREACH_X(iter, info.items_, OB_SUCC(ret)) {
      if (OB_FAIL(find_item_(iter->name_, found, old_item))) {
        LOG_WARN("fail to find item", K(ret), KPC(iter));
      } else if (!found) {
        if (OB_FAIL(this->add_item(*iter))) {
          LOG_WARN("fail to add item", K(ret), KPC(iter));
        }
      } else if (OB_ISNULL(old_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old_item is nullptr", K(ret));
      } else if (OB_FAIL(old_item->update(*iter))) {
        LOG_WARN("fail to update item", K(ret), KPC(iter));
      }
    }
  }
  return ret;
}

int ObTransferPerfDiagInfo::update(const ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info is invalid", K(ret), K(info));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE != info.module_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected info module", K(ret), K(info.module_));
  } else {
    const ObTransferPerfDiagInfo &diag_info = static_cast<const ObTransferPerfDiagInfo &>(info);
    if (OB_FAIL(update_item_list_(diag_info))) {
      LOG_WARN("fail to update item list", K(ret), K(diag_info));
    } else if (OB_ISNULL(task_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task_ is nullptr", K(ret));
    } else {
      timestamp_ = 0 == timestamp_ ? diag_info.timestamp_ : timestamp_;
      end_timestamp_ = diag_info.end_timestamp_;
      task_->key_.retry_id_ = diag_info.items_.get_first()->retry_id_;
      retry_id_ = diag_info.items_.get_first()->retry_id_;
    }
  }
  return ret;
}

bool ObTransferPerfDiagInfo::is_inited() const
{
  return is_inited_;
}

int ObTransferPerfDiagInfo::copy_item_list(const ObTransferPerfDiagInfo &info)
{
  int ret = OB_SUCCESS;
  ObIStorageHACostItem *copy_item = nullptr;
  if (!info.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info is not inited", K(ret));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    DLIST_FOREACH_X(iter, info.items_, OB_SUCC(ret)) {
      if (OB_FAIL(iter->deep_copy(*allocator_, copy_item))) {
        LOG_WARN("fail to deep copy item", K(ret), KPC(iter));
      } else if (!items_.add_last(copy_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add item", K(ret), KPC(copy_item));
      } else if (OB_FAIL(item_map_.set_refactored(static_cast<int>(iter->name_), copy_item, 0/*is_overwrite*/))) {
        LOG_WARN("fail to add item to map", K(ret), KPC(iter));
      } else {
        item_size_ += copy_item->get_size();
      }
    }
  }
  return ret;
}

int ObTransferPerfDiagInfo::deep_copy(ObIAllocator &allocator, ObStorageHADiagInfo *&out_info) const
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  out_info = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTransferPerfDiagInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    ObTransferPerfDiagInfo *info = nullptr;
    if (OB_ISNULL(info = (new (buf) ObTransferPerfDiagInfo()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item is nullptr", K(ret));
    } else {
      buf = nullptr;
      if (OB_FAIL(info->init(&allocator, MTL_ID()))) {
        LOG_WARN("fail to init transfer perf info", K(ret));
      } else if (OB_FAIL(info->copy_item_list(*this))) {
        LOG_WARN("fail to assign item list", K(ret), K(info));
      } else if (OB_FAIL(info->assign(*this))) {
        LOG_WARN("failed to assign info", K(ret), KPC(this));
      } else {
        out_info = info;
        info = nullptr;
      }
      if (OB_NOT_NULL(info)) {
        info->~ObTransferPerfDiagInfo();
        allocator.free(info);
        info = nullptr;
      }
    }
    if (nullptr != buf) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObTransferPerfDiagInfo::get_info(char *str, const int64_t size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t pos = 0;
    memset(str, 0, size);
    DLIST_FOREACH_X(item, items_, OB_SUCC(ret)) {
      //unset error code for print Truncated info
      if (pos + strlen(item->get_item_name()) >= size) {
        LOG_WARN("size overwrite", K(ret), K(pos), K(item), "item_len", strlen(item->get_item_name()), K(size));
        break;
      } else if (OB_FAIL(item->get_str(str, size, pos))) {
        LOG_WARN("fail to get info", K(ret), K(size), K(pos));
      }
    }
  }
  return ret;
}

int ObTransferPerfDiagInfo::get_task_id(char *str, const int64_t size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t pos = 0;
    memset(str, 0, size);
    if (OB_FAIL(databuff_printf(str, size, pos, "%ld", task_id_))) {
      LOG_WARN("failed to print task_id.", K(ret), K(size), K(pos), K(task_id_));
    }
  }
  return ret;
}

int ObTransferPerfDiagInfo::assign(const ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE != info.module_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info module type is unexpected", K(ret), K(info.module_));
  } else if (OB_FAIL(ObStorageHADiagInfo::assign(info))) {
    LOG_WARN("fail to assign info", K(ret), K(info));
  } else {
    const ObTransferPerfDiagInfo &diag_info = static_cast<const ObTransferPerfDiagInfo &>(info);
    end_timestamp_ = diag_info.end_timestamp_;
    tablet_count_ = diag_info.tablet_count_;
  }
  return ret;
}