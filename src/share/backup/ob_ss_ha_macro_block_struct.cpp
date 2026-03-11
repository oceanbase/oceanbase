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

#include "ob_ss_ha_macro_block_struct.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/scn.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::share;

/* ObSSHAMacroTaskType */
bool ObSSHAMacroTaskType::is_valid() const
{
  return type_ >= Type::BACKUP && type_ < Type::MAX_TYPE;
}

const char* ObSSHAMacroTaskType::get_str() const
{
  const char *str = "UNKNOWN";
  const char *type_strs[] = {
    "BACKUP",
    "RESTORE",
    "BACKUP_CLEAN",
  };
  STATIC_ASSERT(MAX_TYPE == ARRAYSIZEOF(type_strs), "type count mismatch");
  if (type_ < BACKUP || type_ >= MAX_TYPE) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid ObSSHAMacroTaskType type", K(type_));
  } else {
    str = type_strs[type_];
  }
  return str;
}

int ObSSHAMacroTaskType::from_string(const common::ObString &task_type_str, ObSSHAMacroTaskType &task_type)
{
  int ret = OB_SUCCESS;
  if (task_type_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task type string is empty", K(ret));
  } else if (0 == task_type_str.case_compare("backup")) {
    task_type = ObSSHAMacroTaskType(Type::BACKUP);
  } else if (0 == task_type_str.case_compare("restore")) {
    task_type = ObSSHAMacroTaskType(Type::RESTORE);
  } else if (0 == task_type_str.case_compare("backup_clean")) {
    task_type = ObSSHAMacroTaskType(Type::BACKUP_CLEAN);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task type string", K(ret), K(task_type_str));
  }
  return ret;
}

ObSSHAMacroBlockTaskAttr::ObSSHAMacroBlockTaskAttr()
  : tenant_id_(OB_INVALID_TENANT_ID),
    parent_task_id_(0),
    ls_id_(),
    task_id_(0),
    macro_id_list_(),
    macro_block_cnt_(),
    status_(),
    start_time_(0),
    end_time_(0),
    svr_addr_(),
    trace_id_(),
    result_(OB_SUCCESS),
    comment_(),
    retry_cnt_(0),
    total_bytes_(0),
    finish_bytes_(0)
{
}

bool ObSSHAMacroBlockTaskAttr::is_valid() const
{
  bool valid = OB_INVALID_TENANT_ID != tenant_id_
         && parent_task_id_ > 0
         && ls_id_.is_valid()
         && task_id_ > 0
         && task_type_.is_valid()
         && status_.is_valid();

  if (!valid) {
  } else if (task_type_.is_backup_clean()) {
    valid = macro_id_list_.is_macro_list_addr() && macro_id_list_.get_macro_list_addr().is_valid();
  } else {
    valid = macro_id_list_.is_macro_block_batch()
         && !macro_id_list_.get_macro_block_batch().is_empty()
         && macro_block_cnt_ > 0;
  }
  return valid;
}

void ObSSHAMacroBlockTaskAttr::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  parent_task_id_ = 0;
  ls_id_.reset();
  task_id_ = 0;
  macro_id_list_.reset();
  macro_block_cnt_ = 0;
  start_time_ = 0;
  end_time_ = 0;
  svr_addr_.reset();
  trace_id_.reset();
  result_ = OB_SUCCESS;
  comment_.reset();
  retry_cnt_ = 0;
  total_bytes_ = 0;
  finish_bytes_ = 0;
}

int ObSSHAMacroBlockTaskAttr::assign(const ObSSHAMacroBlockTaskAttr &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    parent_task_id_ = other.parent_task_id_;
    ls_id_ = other.ls_id_;
    task_id_ = other.task_id_;
    task_type_ = other.task_type_;
    status_ = other.status_;

    if (OB_FAIL(macro_id_list_.assign(other.macro_id_list_))) {
      LOG_WARN("failed to assign macro_id_list", K(ret));
    } else {
      macro_block_cnt_ = other.macro_block_cnt_;
      start_time_ = other.start_time_;
      end_time_ = other.end_time_;
      svr_addr_ = other.svr_addr_;

      if (OB_FAIL(trace_id_.assign(other.trace_id_))) {
        LOG_WARN("failed to write trace id string", K(ret));
      } else if (OB_FAIL(comment_.assign(other.comment_))) {
        LOG_WARN("failed to write comment string", K(ret));
      } else {
        result_ = other.result_;
        retry_cnt_ = other.retry_cnt_;
        total_bytes_ = other.total_bytes_;
        finish_bytes_ = other.finish_bytes_;
      }
    }
  }
  return ret;
}

int ObSSHAMacroBlockTaskAttr::serialize_macro_id_list(
    common::ObIAllocator &allocator,
    common::ObString &binary_data) const
{
  int ret = OB_SUCCESS;
  const int64_t binary_len = macro_id_list_.get_serialize_size();
  char *binary_buf = static_cast<char *>(allocator.alloc(binary_len));
  int64_t pos = 0;

  if (OB_ISNULL(binary_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buffer", K(ret), K(binary_len));
  } else if (OB_FAIL(macro_id_list_.serialize(binary_buf, binary_len, pos))) {
    LOG_WARN("failed to serialize", K(ret));
  } else {
    // Return binary data directly. Caller wraps with ObHexEscapeSqlStr,
    // sql_append_hex_escape_str() generates X'...' format, SQL parser converts back to binary.
    binary_data.assign_ptr(binary_buf, pos);
  }
  return ret;
}

int ObSSHAMacroBlockTaskAttr::deserialize_macro_id_list(
    const common::ObString &binary_data)
{
  int ret = OB_SUCCESS;
  if (binary_data.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("binary data is empty", K(ret));
  } else {
    // SQL parser already converted X'...' to binary data, just deserialize directly
    int64_t pos = 0;
    if (OB_FAIL(macro_id_list_.deserialize(binary_data.ptr(), binary_data.length(), pos))) {
      LOG_WARN("failed to deserialize", K(ret));
    }
  }
  return ret;
}

int ObSSHAMacroBlockTaskAttr::init_from_rpc_arg(const obrpc::ObSSHAMacroBlocksArg &arg)
{
  int ret = OB_SUCCESS;
  tenant_id_ = arg.tenant_id_;
  parent_task_id_ = arg.job_id_;
  ls_id_ = share::ObLSID(arg.ls_id_);
  task_id_ = arg.task_id_;
  task_type_.type_ = static_cast<ObSSHAMacroTaskType::Type>(arg.task_type_);
  retry_cnt_ = arg.retry_cnt_;
  status_ = share::ObBackupTaskStatus::DOING;
  start_time_ = ObTimeUtil::current_time();
  svr_addr_ = GCTX.self_addr();
  macro_block_cnt_ = arg.macro_block_cnt_;
  result_ = OB_SUCCESS;

  char trace_id_buf[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  int64_t pos = arg.trace_id_.to_string(trace_id_buf, sizeof(trace_id_buf));
  if (pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert trace id to string", K(ret), K(arg.trace_id_), K(pos));
  } else if (OB_FAIL(trace_id_.assign(trace_id_buf))) {
    LOG_WARN("failed to assign trace id", K(ret));
  } else if (task_type_.is_backup_clean()) {
    // BACKUP_CLEAN: use macro_list_addr
    macro_id_list_.set_macro_list_addr(arg.macro_list_addr_);
  } else if (OB_FAIL(macro_id_list_.set_macro_block_batch(arg.macro_block_batch_))) {
    // BACKUP/RESTORE: use macro_block_batch (default type)
    LOG_WARN("failed to set macro block batch", K(ret), K(arg));
  }
  return ret;
}
int ObSSHAMacroBlockTaskAttr::set_task_type(const common::ObString &task_type)
{
  return ObSSHAMacroTaskType::from_string(task_type, task_type_);
}

const char* ObSSHAMacroBlockTaskAttr::get_task_type_str() const
{
  return task_type_.get_str();
}

int ObSSHAMacroBlockTaskAttr::set_task_status(const common::ObString &task_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(status_.set_status(task_status))) {
    LOG_WARN("failed to set status", K(ret), K(task_status));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSHAMacroBlockInfo, ls_id_, macro_block_id_, occupy_size_);

ObSSHAMacroBlockInfo::ObSSHAMacroBlockInfo()
  : ls_id_(), macro_block_id_(), occupy_size_(0)
{
}

bool ObSSHAMacroBlockInfo::is_valid() const
{
  return ls_id_.is_valid() && macro_block_id_.is_valid() && occupy_size_ >= 0;
}

void ObSSHAMacroBlockInfo::reset()
{
  ls_id_.reset();
  macro_block_id_.reset();
  occupy_size_ = 0;
}

int ObSSHAMacroBlockInfo::assign(const ObSSHAMacroBlockInfo &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    ls_id_ = other.ls_id_;
    macro_block_id_ = other.macro_block_id_;
    occupy_size_ = other.occupy_size_;
  }
  return ret;
}

int ObSSHAMacroBlockInfo::hash(uint64_t &hash_val) const
{
  return macro_block_id_.hash(hash_val);
}

OB_SERIALIZE_MEMBER(ObSSHAMacroBlockBatch, macro_blocks_, total_bytes_);
ObSSHAMacroBlockBatch::ObSSHAMacroBlockBatch()
  : macro_blocks_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("HaMacroBlkBat")), total_bytes_(0)
{
}

void ObSSHAMacroBlockBatch::reset()
{
  macro_blocks_.reset();
  total_bytes_ = 0;
}

int ObSSHAMacroBlockBatch::assign(const ObSSHAMacroBlockBatch &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(macro_blocks_.assign(other.macro_blocks_))) {
      LOG_WARN("failed to assign macro blocks", K(ret));
    } else {
      total_bytes_ = other.total_bytes_;
    }
  }
  return ret;
}

bool ObSSHAMacroBlockBatch::is_valid() const
{
  return !macro_blocks_.empty() && total_bytes_ >= 0;
}

int64_t ObSSHAMacroBlockBatch::get_total_bytes() const
{
  if (OB_UNLIKELY(0 == total_bytes_ && !macro_blocks_.empty())) {
    // This can happen when occupy_size_ is not set in ObSSHAMacroBlockInfo (e.g. restore path).
    // Callers using this for progress tracking should handle 0 as "unknown".
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "total_bytes is 0 but batch is not empty, occupy_size may not be set",
              "macro_block_count", macro_blocks_.count());
  }
  return total_bytes_;
}

int ObSSHAMacroBlockBatch::get_ls_id(share::ObLSID &ls_id) const
{
  int ret = OB_SUCCESS;
  if (macro_blocks_.empty()) {
    ret = OB_EMPTY_RESULT;
    LOG_WARN("macro blocks is empty", K(ret));
  } else {
    ls_id = macro_blocks_.at(0).ls_id_;
  }
  return ret;
}

int ObSSHAMacroBlockBatch::add_one(const ObSSHAMacroBlockInfo &macro_block_info)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  if (!macro_block_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block info", K(ret), K(macro_block_info));
  } else if (macro_blocks_.empty()) {
    ls_id = macro_block_info.ls_id_;
  } else if (OB_FAIL(get_ls_id(ls_id))) {
    LOG_WARN("fail to get ls id", K(ret));
  } else if (ls_id != macro_block_info.ls_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro block info has different ls_id", K(ret), K(ls_id), K(macro_block_info));
  }

  if (FAILEDx(macro_blocks_.push_back(macro_block_info))) {
    LOG_WARN("failed to add macro block info", K(ret), K(macro_block_info));
  } else {
    total_bytes_ += macro_block_info.occupy_size_;
  }
  return ret;
}

/* ObSSHAMacroIdList */
OB_SERIALIZE_MEMBER(ObSSHAMacroIdList, type_, macro_block_batch_, macro_list_addr_);
ObSSHAMacroIdList::ObSSHAMacroIdList()
  : type_(MACRO_BLOCK_BATCH),
    macro_block_batch_(),
    macro_list_addr_()
{
}

int ObSSHAMacroIdList::set_macro_block_batch(const ObSSHAMacroBlockBatch &batch)
{
  type_ = MACRO_BLOCK_BATCH;
  return macro_block_batch_.assign(batch);
}

void ObSSHAMacroIdList::set_macro_list_addr(const backup::ObBackupBlockFileAddr &addr)
{
  type_ = MACRO_LIST_ADDR;
  macro_list_addr_ = addr;
}

void ObSSHAMacroIdList::reset()
{
  macro_block_batch_.reset();
  macro_list_addr_.reset();
}

int ObSSHAMacroIdList::assign(const ObSSHAMacroIdList &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    type_ = other.type_;
    if (OB_FAIL(macro_block_batch_.assign(other.macro_block_batch_))) {
      LOG_WARN("failed to assign macro_block_batch", K(ret));
    } else {
      macro_list_addr_ = other.macro_list_addr_;
    }
  }
  return ret;
}

ObMacroBlockTaskScheduleParam::ObMacroBlockTaskScheduleParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    backup_set_id_(0),
    job_id_(0),
    backup_dest_(),
    batch_size_(1024),               // Default 1024 macro blocks per batch
    max_parallel_degree_(8),         // Default maximum 8 parallel tasks
    snapshot_scn_()
{
}

bool ObMacroBlockTaskScheduleParam::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && backup_set_id_ > 0
         && job_id_ > 0
         && backup_dest_.is_valid()
         && batch_size_ > 0
         && max_parallel_degree_ > 0
         && snapshot_scn_.is_valid();
}

int ObMacroBlockTaskScheduleParam::assign(const ObMacroBlockTaskScheduleParam &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    backup_set_id_ = other.backup_set_id_;
    job_id_ = other.job_id_;
    batch_size_ = other.batch_size_;
    max_parallel_degree_ = other.max_parallel_degree_;
    snapshot_scn_ = other.snapshot_scn_;

    if (OB_FAIL(backup_dest_.deep_copy(other.backup_dest_))) {
      LOG_WARN("failed to assign backup dest", K(ret));
    }
  }
  return ret;
}

// ObSSHAMacroTaskMgrState implementation
ObSSHAMacroTaskMgrState::ObSSHAMacroTaskMgrState()
{
  reset();
}

void ObSSHAMacroTaskMgrState::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  parent_task_id_ = 0;
  task_status_ = 0;
  task_type_ = ObSSHAMacroTaskType();
  total_task_count_ = 0;
  finish_task_count_ = 0;
  total_macro_block_count_ = 0;
  finish_macro_block_count_ = 0;
  total_bytes_ = 0;
  finish_bytes_ = 0;
  result_ = OB_SUCCESS;
  comment_.reset();
}

int ObSSHAMacroTaskMgrState::assign(const ObSSHAMacroTaskMgrState &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    parent_task_id_ = other.parent_task_id_;
    task_status_ = other.task_status_;
    task_type_ = other.task_type_;
    total_task_count_ = other.total_task_count_;
    finish_task_count_ = other.finish_task_count_;
    total_macro_block_count_ = other.total_macro_block_count_;
    finish_macro_block_count_ = other.finish_macro_block_count_;
    total_bytes_ = other.total_bytes_;
    finish_bytes_ = other.finish_bytes_;
    result_ = other.result_;

    if (OB_FAIL(comment_.assign(other.comment_))) {
      LOG_WARN("failed to copy comment string", K(ret));
    }
  }
  return ret;
}

bool ObSSHAMacroTaskMgrState::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && parent_task_id_ > 0
      && task_status_ >= 0
      && total_task_count_ >= 0
      && finish_task_count_ >= 0
      && total_macro_block_count_ >= 0
      && finish_macro_block_count_ >= 0
      && total_bytes_ >= 0
      && finish_bytes_ >= 0;
}