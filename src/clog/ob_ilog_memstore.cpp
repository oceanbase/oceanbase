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

#include "ob_ilog_memstore.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/utility/utility.h"

namespace oceanbase {
using namespace common;
namespace clog {

ObIlogMemstore::ObIlogMemstore()
    : is_inited_(false),
      partition_meta_info_(),
      partition_memberlist_info_(),
      log_cursor_ext_info_(),
      create_ts_(OB_INVALID_TIMESTAMP),
      cursor_size_(0),
      clog_size_(0)
{}

ObIlogMemstore::~ObIlogMemstore()
{
  destroy();
}

int ObIlogMemstore::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CSR_LOG(ERROR, "ObIlogMemstore init twice", K(ret));
  } else if (OB_FAIL(partition_meta_info_.init(ObModIds::OB_ILOG_MEMSTORE_META))) {
    CSR_LOG(WARN, "partition_meta_info_ init failed", K(ret));
  } else if (OB_FAIL(partition_memberlist_info_.init(ObModIds::OB_ILOG_MEMSTORE_META))) {
    CSR_LOG(WARN, "partition_memberlist_info_ init failed", K(ret));
  } else if (OB_FAIL(log_cursor_ext_info_.init(ObModIds::OB_ILOG_MEMSTORE_CURSOR))) {
    CSR_LOG(WARN, "log_cursor_ext_info_ init failed", K(ret));
  } else {
    create_ts_ = ObTimeUtility::current_time();
    cursor_size_ = 0;
    clog_size_ = 0;
    is_inited_ = true;
  }

  if (!is_inited_) {
    destroy();
    CSR_LOG(WARN, "ObIlogMemstore init failed", K(ret));
  } else {
    CSR_LOG(INFO, "ObIlogMemstore init finished", K(ret));
  }
  return ret;
}

void ObIlogMemstore::destroy()
{
  is_inited_ = false;
  partition_meta_info_.destroy();
  partition_memberlist_info_.destroy();
  log_cursor_ext_info_.destroy();
  create_ts_ = OB_INVALID_TIMESTAMP;
  cursor_size_ = 0;
  clog_size_ = 0;
}

int ObIlogMemstore::check_need_freeze(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, ObIlogFreezeTriggerType& trigger_type) const
{
  int ret = OB_SUCCESS;
  trigger_type = OB_INVALID_TRIGGER_TYPE;
  bool log_continuous = false;
  int64_t freeze_trigger_us = TIME_TRIGGER_US;
  int64_t cursor_size_trigger_cfg = CURSOR_SIZE_TRIGGER;
  int64_t clog_size_trigger_cfg = CLOG_SIZE_TRIGGER;
#ifdef ERRSIM
  freeze_trigger_us = ObServerConfig::get_instance().ilog_flush_trigger_time;
  cursor_size_trigger_cfg = 128;
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(log_id));
  } else if (OB_FAIL(check_log_continuous_(partition_key, log_id, log_continuous))) {
    CSR_LOG(ERROR, "check_log_continuous_ failed, unexpected", K(ret), K(partition_key), K(log_id), K(log_continuous));
  } else {

    const bool log_continuous_trigger = !log_continuous;
    const bool cursor_size_trigger = (ATOMIC_LOAD(&cursor_size_) >= cursor_size_trigger_cfg);
    const bool clog_size_trigger = (ATOMIC_LOAD(&clog_size_) >= clog_size_trigger_cfg);
    const bool time_trigger =
        ((ObTimeUtility::current_time() - create_ts_) >= freeze_trigger_us) && (ATOMIC_LOAD(&cursor_size_) > 0);

    if (log_continuous_trigger) {
      trigger_type = OB_ILOG_NOT_CONTINOUS_TRIGGER_TYPE;
    } else if (cursor_size_trigger) {
      trigger_type = OB_ILOG_SIZE_TRIGGER_TYPE;
    } else if (clog_size_trigger) {
      trigger_type = OB_CLOG_SIZE_TRIGGER_TYPE;
    } else if (time_trigger) {
      trigger_type = OB_TIMER_TRIGGER_TYPE;
    } else {
      trigger_type = OB_NO_NEED_FREEZE_TRIGGER_TYPE;
    }

    CSR_LOG(TRACE,
        "check_need_freeze",
        K(trigger_type),
        K(log_continuous_trigger),
        K(cursor_size_trigger),
        K(clog_size_trigger),
        K(time_trigger),
        K(cursor_size_),
        K(clog_size_),
        K(create_ts_));
  }

  if (OB_SUCCESS != ret) {
    CSR_LOG(ERROR, "check_need_freeze return value upexpected", K(ret), K(partition_key), K(log_id));
  }
  CSR_LOG(TRACE, "check_need_freeze", K(ret), K(partition_key), K(log_id), K(trigger_type));
  return ret;
}

int ObIlogMemstore::timer_check_need_freeze(bool& need_freeze) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else {
    int64_t freeze_trigger_us = TIME_TRIGGER_US;
#ifdef ERRSIM
    freeze_trigger_us = ObServerConfig::get_instance().ilog_flush_trigger_time;
#endif
    const bool time_trigger =
        (((ObTimeUtility::current_time() - create_ts_) >= freeze_trigger_us) && (ATOMIC_LOAD(&cursor_size_) > 0));
    need_freeze = time_trigger;
    if (!need_freeze) {
      CSR_LOG(TRACE,
          "timer_check_need_freeze",
          K(need_freeze),
          K(time_trigger),
          K(create_ts_),
          K(cursor_size_),
          K(freeze_trigger_us));
    } else {
      CSR_LOG(INFO,
          "timer_check_need_freeze",
          K(need_freeze),
          K(time_trigger),
          K(create_ts_),
          K(cursor_size_),
          K(freeze_trigger_us));
    }
  }
  if (OB_SUCCESS != ret) {
    CSR_LOG(ERROR, "timer_check_need_freeze return value unexpected", K(ret));
  }
  CSR_LOG(TRACE, "timer_check_need_freeze", K(ret), K(need_freeze));
  return ret;
}

int ObIlogMemstore::check_need_switch_file(bool& need_switch_file) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else {
    int64_t cursor_size_trigger_cfg = CURSOR_SIZE_TRIGGER;
#ifdef ERRSIM
    cursor_size_trigger_cfg = 128;
#endif
    need_switch_file =
        (ATOMIC_LOAD(&cursor_size_) >= cursor_size_trigger_cfg || ATOMIC_LOAD(&clog_size_) >= CLOG_SIZE_TRIGGER);
  }
  return ret;
}

int ObIlogMemstore::get_cursor_size(int64_t& cursor_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "get_cursor_size failed", K(ret));
  } else {
    cursor_size = cursor_size_;
  }
  return ret;
}

int ObIlogMemstore::insert_partition_meta_info(const common::ObPartitionKey& pkey, const IndexInfoBlockEntry& entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_meta_info_.insert(pkey, entry))) {
    CLOG_LOG(ERROR, "insert into partition_meta_info_ failed", K(ret), K(pkey), K(entry));
  }
  return ret;
}

int ObIlogMemstore::insert_partition_memberlist_info(
    const common::ObPartitionKey& pkey, const MemberListInfo& member_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_memberlist_info_.insert(pkey, member_list))) {
    CLOG_LOG(ERROR, "insert into partition_memberlist_info_ failed", K(ret), K(pkey), K(member_list));
  }
  return ret;
}

int ObIlogMemstore::insert_partition_log_cursor_ext_info(
    const ObPartitionLogInfo& log_info, const ObLogCursorExt& log_cursor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_cursor_ext_info_.insert(log_info, log_cursor))) {
    CLOG_LOG(ERROR, "insert into partition_log_cursor_info_ failed", K(ret), K(log_info), K(log_cursor));
  } else {
    cursor_size_ += sizeof(ObLogCursorExt);
    clog_size_ += log_cursor.get_size();
  }
  return ret;
}

int ObIlogMemstore::check_log_continuous_(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, bool& log_continuous) const
{
  int ret = OB_SUCCESS;
  uint64_t ret_max_log_id = OB_INVALID_ID;
  int64_t ret_max_log_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_max_log_id_and_ts(partition_key, ret_max_log_id, ret_max_log_ts)) && OB_PARTITION_NOT_EXIST != ret) {
    log_continuous = false;
    CSR_LOG(ERROR, "get_max_log_id_and_ts failed", K(ret), K(partition_key), K(log_id));
  } else if (OB_PARTITION_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    log_continuous = true;
    CSR_LOG(
        TRACE, "the first log of this partition for this memstore, continuous", K(ret), K(partition_key), K(log_id));
  } else {
    log_continuous = (log_id == ret_max_log_id + 1);
  }

  if (!log_continuous) {
    CSR_LOG(INFO, "log is not continuous", K(ret), K(partition_key), K(log_id), K(ret_max_log_id));
  }
  return ret;
}

int ObIlogMemstore::submit_cursor(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id) || !log_cursor_ext.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  } else {
    while (true) {
      if (OB_FAIL(submit_cursor_(partition_key, log_id, log_cursor_ext))) {
        CSR_LOG(ERROR, "submit_cursor_ failed, need retry", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
        usleep(1000);
      } else {
        break;
      }
    }
  }

  if (OB_SUCCESS != ret) {
    CSR_LOG(ERROR, "submit_cursor return value unexpected", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  }
  CSR_LOG(TRACE, "submit_cursor", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  return ret;
}

int ObIlogMemstore::submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
    const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
    const int64_t memberlist_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id) || !log_cursor_ext.is_valid() ||
             !memberlist.is_valid() || replica_num <= 0 || memberlist_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR,
        "invalid arguments",
        K(ret),
        K(partition_key),
        K(log_id),
        K(log_cursor_ext),
        K(memberlist),
        K(replica_num),
        K(memberlist_version));
  } else if (OB_FAIL(submit_cursor(partition_key, log_id, log_cursor_ext))) {
    CSR_LOG(ERROR, "submit_cursor failed", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  } else if (OB_FAIL(submit_memberlist_info_with_retry_(
                 partition_key, memberlist, replica_num, log_id, memberlist_version))) {
    CSR_LOG(ERROR,
        "submit_memberlist_info_with_retry_ failed",
        K(ret),
        K(partition_key),
        K(memberlist),
        K(replica_num),
        K(log_id),
        K(memberlist_version));
  } else {
    // do nothing
  }

  if (OB_SUCCESS != ret) {
    CSR_LOG(ERROR,
        "submit_cursor(with memberlist info) return value unexpected",
        K(ret),
        K(partition_key),
        K(log_id),
        K(log_cursor_ext),
        K(memberlist),
        K(replica_num),
        K(memberlist_version));
  }
  CSR_LOG(TRACE,
      "submit_cursor(with memberlist info)",
      K(ret),
      K(partition_key),
      K(log_id),
      K(log_cursor_ext),
      K(memberlist),
      K(replica_num),
      K(memberlist_version));
  return ret;
}

int ObIlogMemstore::submit_cursor_(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionLogInfo partition_log_info(partition_key, log_id);
  if (OB_FAIL(log_cursor_ext_info_.insert(partition_log_info, log_cursor_ext))) {
    CSR_LOG(ERROR, "log_cursor_ext_info_ insert failed", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  } else {
    // For same partition, there is only one thread can submit_cursor_
    IndexInfoBlockEntry index_info_block_entry;
    PartitionMetaInfoFunctor fn(log_id, log_cursor_ext);
    if (OB_FAIL(partition_meta_info_.get(partition_key, index_info_block_entry)) && OB_ENTRY_NOT_EXIST != ret) {
      CSR_LOG(ERROR, "partition_meta_info_ get failed", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      index_info_block_entry.min_log_id_ = log_id;
      index_info_block_entry.max_log_id_ = log_id;
      index_info_block_entry.min_log_timestamp_ = log_cursor_ext.get_submit_timestamp();
      index_info_block_entry.max_log_timestamp_ = log_cursor_ext.get_submit_timestamp();
      index_info_block_entry.start_offset_ = OB_INVALID_OFFSET;

      if (OB_FAIL(partition_meta_info_.insert(partition_key, index_info_block_entry))) {
        CSR_LOG(ERROR, "partition_meta_info_ insert failed", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
      }
    } else if (OB_FAIL(partition_meta_info_.operate(partition_key, fn))) {
      CSR_LOG(ERROR, "partition_meta_info_ operate failed", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
    } else {
      // do nothing
    }

    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = log_cursor_ext_info_.erase(partition_log_info))) {
        CSR_LOG(ERROR, "log_cursor_ext_info_ erase failed", K(tmp_ret), K(partition_key), K(log_id), K(log_cursor_ext));
      }
    } else {
      ATOMIC_FAA(&cursor_size_, sizeof(ObLogCursorExt));
      ATOMIC_FAA(&clog_size_, log_cursor_ext.get_size());
    }
  }
  return ret;
}

int ObIlogMemstore::submit_memberlist_info_with_retry_(const common::ObPartitionKey& partition_key,
    const common::ObMemberList& memberlist, const int64_t replica_num, const uint64_t membership_log_id,
    const int64_t memberlist_version)
{
  int ret = OB_SUCCESS;
  while (true) {
    if (OB_FAIL(
            submit_memberlist_info_(partition_key, memberlist, replica_num, membership_log_id, memberlist_version))) {
      CSR_LOG(ERROR,
          "submit_memberlist_info_ failed, need retry",
          K(ret),
          K(partition_key),
          K(memberlist),
          K(replica_num),
          K(membership_log_id),
          K(memberlist_version));
      usleep(1000);
    } else {
      break;
    }
  }
  return ret;
}

int ObIlogMemstore::submit_memberlist_info_(const common::ObPartitionKey& partition_key,
    const common::ObMemberList& memberlist, const int64_t replica_num, const uint64_t membership_log_id,
    const int64_t memberlist_version)
{
  int ret = OB_SUCCESS;
  MemberListInfo memberlist_info;
  MemberListInfoFunctor fn(memberlist, replica_num, membership_log_id, memberlist_version);
  if (OB_FAIL(partition_memberlist_info_.get(partition_key, memberlist_info)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "partition_memberlist_info_ get failed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    memberlist_info.set_memberlist_info(memberlist, replica_num, membership_log_id, memberlist_version);
    if (OB_FAIL(partition_memberlist_info_.insert(partition_key, memberlist_info))) {
      CSR_LOG(ERROR,
          "partition_memberlist_info_ insert failed",
          K(ret),
          K(partition_key),
          K(memberlist),
          K(replica_num),
          K(membership_log_id),
          K(memberlist_version));
    }
  } else if (OB_FAIL(partition_memberlist_info_.operate(partition_key, fn))) {
    CSR_LOG(ERROR,
        "partition_memberlist_info_ operate failed",
        K(ret),
        K(partition_key),
        K(memberlist),
        K(replica_num),
        K(membership_log_id),
        K(memberlist_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogMemstore::get_cursor(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, ObLogCursorExt& log_cursor_ext) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionLogInfo partition_log_info(partition_key, log_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(log_id));
  } else if (OB_FAIL(log_cursor_ext_info_.get(partition_log_info, log_cursor_ext)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "log_cursor_ext_info_ get failed", K(ret), K(partition_key), K(log_id));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    uint64_t ret_min_log_id = OB_INVALID_ID;
    int64_t ret_min_log_ts = OB_INVALID_TIMESTAMP;
    if (OB_SUCCESS != (tmp_ret = get_min_log_id_and_ts(partition_key, ret_min_log_id, ret_min_log_ts)) &&
        OB_PARTITION_NOT_EXIST != tmp_ret) {
      CSR_LOG(ERROR, "get_max_log_id_and_ts failed", K(tmp_ret), K(partition_key));
    } else if (OB_PARTITION_NOT_EXIST == tmp_ret) {
      // ret = OB_ENTRY_NOT_EXIST;
    } else if (log_id >= ret_min_log_id) {
      // If log_id is greater than lower bound, and the log doesn't exist
      // in log_cursor_ext_info_, return UPPER_BOUND
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
    }
  } else {
    // do nothing
  }

  if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret) {
    CSR_LOG(ERROR, "get_cursor return value unexpected", K(ret), K(partition_key), K(log_id));
  }
  CSR_LOG(TRACE, "get_cursor", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  return ret;
}

int ObIlogMemstore::get_max_log_id_and_ts(
    const common::ObPartitionKey& partition_key, uint64_t& ret_max_log_id, int64_t& ret_max_log_ts) const
{
  int ret = OB_SUCCESS;
  IndexInfoBlockEntry index_info_block_entry;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(partition_meta_info_.get(partition_key, index_info_block_entry)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "partition_meta_info_ get failed", K(ret), K(partition_key));
  } else if (OB_SUCCESS == ret) {
    ret_max_log_id = index_info_block_entry.max_log_id_;
    ret_max_log_ts = index_info_block_entry.max_log_timestamp_;
  } else {
    ret = OB_PARTITION_NOT_EXIST;
  }

  if (OB_SUCCESS != ret && OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "get_max_log_id_and_ts return value unexpected", K(ret), K(partition_key));
  }
  CSR_LOG(TRACE, "get_max_log_id_and_ts", K(ret), K(partition_key), K(ret_max_log_id), K(ret_max_log_ts));
  return ret;
}

int ObIlogMemstore::get_min_log_id_and_ts(
    const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, int64_t& ret_min_log_ts) const
{
  int ret = OB_SUCCESS;
  IndexInfoBlockEntry index_info_block_entry;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(partition_meta_info_.get(partition_key, index_info_block_entry)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "partition_meta_info_ get failed", K(ret), K(partition_key));
  } else if (OB_SUCCESS == ret) {
    ret_min_log_id = index_info_block_entry.min_log_id_;
    ret_min_log_ts = index_info_block_entry.min_log_timestamp_;
  } else {
    ret = OB_PARTITION_NOT_EXIST;
  }

  if (OB_SUCCESS != ret && OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "get_min_log_id_and_ts return value unexpected", K(ret), K(partition_key));
  }
  CSR_LOG(TRACE, "get_min_log_id_and_ts", K(ret), K(partition_key), K(ret_min_log_id), K(ret_min_log_ts));
  return ret;
}

int ObIlogMemstore::get_log_id_range(
    const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, uint64_t& ret_max_log_id) const
{
  int ret = OB_SUCCESS;
  IndexInfoBlockEntry index_info_block_entry;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(partition_meta_info_.get(partition_key, index_info_block_entry)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "partition_meta_info_ get failed", K(ret), K(partition_key));
  } else if (OB_SUCCESS == ret) {
    ret_min_log_id = index_info_block_entry.min_log_id_;
    ret_max_log_id = index_info_block_entry.max_log_id_;
  } else {
    ret = OB_PARTITION_NOT_EXIST;
  }

  if (OB_SUCCESS != ret && OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "get_log_id_range return value unexpected", K(ret), K(partition_key));
  }
  CSR_LOG(TRACE, "get_log_id_range", K(ret), K(partition_key), K(ret_min_log_id), K(ret_max_log_id));
  return ret;
}

int ObIlogMemstore::get_index_entry(
    const ObPartitionKey& partition_key, IndexInfoBlockEntry& index_info_block_entry) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogMemstore is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(partition_meta_info_.get(partition_key, index_info_block_entry)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "partition_meta_info_ get failed", K(ret), K(partition_key));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_PARTITION_NOT_EXIST;
  }

  if (OB_SUCCESS != ret && OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "get_index_entry return value unexpected", K(ret), K(partition_key));
  }
  CSR_LOG(TRACE, "get_index_entry", K(ret), K(partition_key), K(index_info_block_entry));
  return ret;
}
}  // namespace clog
}  // namespace oceanbase
