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

#include "ob_max_log_meta_info.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObMaxLogMetaInfo::ObMaxLogMetaInfo()
{
  saved_last_gts_ = 0;
  saved_last_get_gts_ts_ = 0;
  total_log_cnt_ = 0;
  total_commit_log_cnt_ = 0;
  reset();
}

ObMaxLogMetaInfo::~ObMaxLogMetaInfo()
{
  reset();
}

int ObMaxLogMetaInfo::init(const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  if (!partition_key.is_valid() || OB_INVALID_ID == log_id || OB_INVALID_TIMESTAMP == timestamp) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(log_id), K(timestamp));
  } else if (log_id >= LOG_ID_LIMIT) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "log_id is larger than LOG_ID_LIMIT", K(ret), K(partition_key), K(log_id));
  } else {
    check_log_id_range_(log_id);
    partition_key_ = partition_key;
    // meta_info_.log_id_ = log_id;
    // meta_info_.offset_ = AGGRE_BUFFER_FLAG;
    // meta_info_.ts_ = timestamp;
    meta_info_.v128_.lo = ((log_id & MASK) << 16) + AGGRE_BUFFER_FLAG;
    meta_info_.v128_.hi = timestamp;
  }

  return ret;
}

void ObMaxLogMetaInfo::reset()
{
  // meta_info_.log_id_ = 0;
  // meta_info_.offset_ = 0;
  // meta_info_.ts_ = 0;
  meta_info_.v128_.lo = 0;
  meta_info_.v128_.hi = 0;
}

uint64_t ObMaxLogMetaInfo::get_log_id() const
{
  MetaInfo meta_info;
  LOAD128(meta_info, &meta_info_);
  return meta_info.log_id_;
}

int64_t ObMaxLogMetaInfo::get_timestamp() const
{
  MetaInfo meta_info;
  LOAD128(meta_info, &meta_info_);
  return meta_info.ts_;
}

int ObMaxLogMetaInfo::get_log_id_and_timestamp(uint64_t& log_id, int64_t& timestamp) const
{
  int ret = OB_SUCCESS;
  MetaInfo meta_info;
  LOAD128(meta_info, &meta_info_);
  log_id = meta_info.log_id_;
  timestamp = meta_info.ts_;
  return ret;
}

int ObMaxLogMetaInfo::alloc_log_id_ts(
    const int64_t base_timestamp, const uint64_t log_id_limit, uint64_t& log_id, int64_t& timestamp, int64_t& offset)
{
  int ret = OB_SUCCESS;

  if (base_timestamp < 0 || log_id_limit >= LOG_ID_LIMIT) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(base_timestamp), K(log_id_limit));
  } else {
    check_log_id_range_(log_id_limit);
    MetaInfo last;
    MetaInfo next;
    while (true) {
      const int64_t base_ts = base_timestamp ? base_timestamp : ObTimeUtility::current_time();
      LOAD128(last, &meta_info_);
      offset = last.offset_;
      // next.log_id_ = last.log_id_ + 1;
      // next.offset_ = AGGRE_BUFFER_FLAG;
      // next.ts_ = (last.ts_ >= (static_cast<uint64_t>(base_ts))) ? (last.ts_ + 1) : base_ts;
      next.v128_.lo = (((last.log_id_ + 1) & MASK) << 16) + AGGRE_BUFFER_FLAG;
      next.v128_.hi = (last.ts_ >= (static_cast<uint64_t>(base_ts))) ? (last.ts_ + 1) : base_ts;
      if (next.log_id_ >= log_id_limit) {
        ret = OB_EAGAIN;
        break;
      } else if (CAS128(&meta_info_, last, next)) {
        break;
      } else {
        PAUSE();
      }
    }
    log_id = next.log_id_;
    timestamp = next.ts_;
    if (timestamp > ObTimeUtility::current_time() + 3600 * 1000 * 1000l) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(
          ERROR, "alloc_log_id_ts return unexpected value", K(last.log_id_), K(last.ts_), K(next.log_id_), K(next.ts_));
    }
  }
  CLOG_LOG(DEBUG, "alloc log id ts", K(ret), K(partition_key_), K(log_id));

  return ret;
}

int ObMaxLogMetaInfo::alloc_log_id_ts(const int64_t base_timestamp, const int64_t size, const uint64_t log_id_limit,
    uint64_t& log_id, int64_t& timestamp, int64_t& offset)
{
  int ret = OB_SUCCESS;

  if (base_timestamp < 0 || size <= 0 || size >= AGGRE_BUFFER_LIMIT || log_id_limit >= LOG_ID_LIMIT) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(base_timestamp), K(size), K(log_id_limit));
  } else {
    check_log_id_range_(log_id_limit);
    ATOMIC_INC(&total_commit_log_cnt_);
    MetaInfo last;
    MetaInfo next;
    while (true) {
      const int64_t base_ts = base_timestamp ? base_timestamp : ObTimeUtility::current_time();
      LOAD128(last, &meta_info_);
      offset = last.offset_;
      if (last.offset_ + size < AGGRE_BUFFER_LIMIT) {
        // next.log_id_ = last.log_id_;
        // next.offset_ = last.offset_ + size;
        // next.ts_ = (last.ts_ >= (static_cast<uint64_t>(base_ts))) ? (last.ts_ + 1) : base_ts;
        next.v128_.lo = ((last.log_id_ & MASK) << 16) + last.offset_ + size;
        next.v128_.hi = (last.ts_ >= (static_cast<uint64_t>(base_ts))) ? (last.ts_ + 1) : base_ts;
        ret = OB_SUCCESS;
      } else {
        // next.log_id_ = last.log_id_ + 1;
        // next.offset_ = size;
        // next.ts_ = (last.ts_ >= (static_cast<uint64_t>(base_ts))) ? (last.ts_ + 1) : base_ts;
        next.v128_.lo = (((last.log_id_ + 1) & MASK) << 16) + size;
        next.v128_.hi = (last.ts_ >= (static_cast<uint64_t>(base_ts))) ? (last.ts_ + 1) : base_ts;
        ret = OB_BLOCK_SWITCHED;
      }

      if (next.log_id_ >= log_id_limit) {
        ret = OB_EAGAIN;
        break;
      } else if (CAS128(&meta_info_, last, next)) {
        break;
      } else {
        PAUSE();
      }
    }
    if (OB_BLOCK_SWITCHED == ret) {
      ATOMIC_INC(&total_log_cnt_);
    }
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      CLOG_LOG(INFO, "alloc_log_id_ts statistics", K(total_log_cnt_), K(total_commit_log_cnt_));
      ATOMIC_STORE(&total_log_cnt_, 0);
      ATOMIC_STORE(&total_commit_log_cnt_, 0);
    }

    log_id = last.log_id_;
    timestamp = next.ts_;
    if (timestamp > ObTimeUtility::current_time() + 3600 * 1000 * 1000l) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(
          ERROR, "alloc_log_id_ts return unexpected value", K(last.log_id_), K(last.ts_), K(next.log_id_), K(next.ts_));
    }
  }
  CLOG_LOG(DEBUG, "alloc log id ts", K(ret), K(partition_key_), K(log_id));

  return ret;
}

int ObMaxLogMetaInfo::try_freeze(const uint64_t log_id, int64_t& offset)
{
  int ret = OB_SUCCESS;

  if (log_id >= LOG_ID_LIMIT) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(log_id));
  } else {
    check_log_id_range_(log_id);
    MetaInfo last;
    MetaInfo next;
    while (true) {
      LOAD128(last, &meta_info_);
      offset = last.offset_;
      if (last.log_id_ != log_id || last.offset_ == AGGRE_BUFFER_FLAG) {
        // no need freeze
        ret = OB_SUCCESS;
        break;
      } else {
        // next.log_id_ = last.log_id_;
        // next.offset_ = AGGRE_BUFFER_FLAG;
        // next.ts_ = last.ts_;
        next.v128_.lo = ((last.log_id_ & MASK) << 16) + AGGRE_BUFFER_FLAG;
        next.v128_.hi = last.ts_;
        if (CAS128(&meta_info_, last, next)) {
          ret = OB_BLOCK_SWITCHED;
          break;
        }
      }
    }
  }
  return ret;
}

int ObMaxLogMetaInfo::try_update_timestamp(const int64_t base_timestamp)
{
  int ret = OB_SUCCESS;

  if (base_timestamp < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(base_timestamp));
  } else {
    MetaInfo last;
    MetaInfo next;
    while (true) {
      LOAD128(last, &meta_info_);
      // next.log_id_ = last.log_id_;
      // next.offset_ = last.offset_;
      // next.ts_ = base_timestamp;
      next.v128_.lo = ((last.log_id_ & MASK) << 16) + last.offset_;
      next.v128_.hi = base_timestamp;
      if (next.ts_ < last.ts_) {
        break;
      } else if (CAS128(&meta_info_, last, next)) {
        break;
      } else {
        PAUSE();
      }
    }
  }

  return ret;
}

int ObMaxLogMetaInfo::try_update_log_id(const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (log_id >= LOG_ID_LIMIT) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(log_id));
  } else {
    check_log_id_range_(log_id);
    MetaInfo last;
    MetaInfo next;
    while (true) {
      LOAD128(last, &meta_info_);
      // next.log_id_ = log_id;
      // next.offset_ = AGGRE_BUFFER_FLAG;
      // next.ts_ = last.ts_;
      next.v128_.lo = ((log_id & MASK) << 16) + AGGRE_BUFFER_FLAG;
      next.v128_.hi = last.ts_;
      if (log_id < last.log_id_) {
        break;
      } else if (CAS128(&meta_info_, last, next)) {
        if (log_id - last.log_id_ >= 50000000) {
          CLOG_LOG(WARN,
              "too large arg log_id",
              K(partition_key_),
              "increased by:",
              log_id - last.log_id_,
              K(log_id),
              K(last.log_id_));
        }
        break;
      } else {
        PAUSE();
      }
    }
  }

  return ret;
}

void ObMaxLogMetaInfo::check_log_id_range_(const uint64_t log_id) const
{
  if (log_id >= LOG_ID_CHECK_RANGE) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(ERROR, "log id may be too large, please attention!", K(log_id), K(partition_key_));
    }
  }
}

ObAggreBuffer::ObAggreBuffer()
{
  reset();
}

ObAggreBuffer::~ObAggreBuffer()
{
  destroy();
}

int ObAggreBuffer::init(const uint64_t id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  ObMemAttr mem_attr(tenant_id, ObModIds::OB_LOG_AGGRE_BUFFER);
  if (OB_INVALID_ID == id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(id));
  } else if (NULL == (data_ = static_cast<char*>(ob_malloc(AGGRE_BUFFER_SIZE, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "alloc memory failed", K(ret), K(id));
  } else {
    id_ = id;
    ref_ = 0;
    // data_ has been initialized
    data_size_ = 0;
    max_submit_timestamp_ = 0;
    last_access_timestamp_ = ObTimeUtility::current_time();
  }

  if (OB_FAIL(ret) && NULL != data_) {
    ob_free(data_);
    data_ = NULL;
  }
  return ret;
}

void ObAggreBuffer::reset()
{
  id_ = OB_INVALID_ID;
  ref_ = 0;
  data_ = NULL;
  data_size_ = 0;
  max_submit_timestamp_ = 0;
  last_access_timestamp_ = ObTimeUtility::current_time();

  ObSpinLockGuard guard(lock_);
  cb_list_head_ = NULL;
  cb_list_tail_ = NULL;
}

void ObAggreBuffer::destroy()
{
  id_ = OB_INVALID_ID;
  ref_ = 0;
  if (NULL != data_) {
    ob_free(data_);
    data_ = NULL;
  }
  data_size_ = 0;
  max_submit_timestamp_ = 0;

  ObSpinLockGuard guard(lock_);
  cb_list_head_ = NULL;
  cb_list_tail_ = NULL;
}

const char* ObAggreBuffer::get_data() const
{
  return data_;
}

int64_t ObAggreBuffer::get_data_size() const
{
  return data_size_;
}

int64_t ObAggreBuffer::get_max_submit_timestamp() const
{
  return max_submit_timestamp_;
}

ObISubmitLogCb* ObAggreBuffer::get_submit_cb() const
{
  return cb_list_head_;
}

int64_t ObAggreBuffer::ref(const int64_t delta)
{
  if (delta < 0) {
    data_size_ = (-delta);
  }
  return ATOMIC_AAF(&ref_, delta);
}

void ObAggreBuffer::fill(
    const int64_t offset, const char* data, const int64_t data_size, const int64_t submit_timestamp, ObISubmitLogCb* cb)
{
  int ret = OB_SUCCESS;
  const int32_t next_log_offset = static_cast<int32_t>((offset + data_size + AGGRE_LOG_RESERVED_SIZE) & 0xffffffff);
  int64_t pos = 0;
  if (OB_FAIL(serialization::encode_i32(data_ + offset, AGGRE_LOG_RESERVED_SIZE, pos, next_log_offset))) {
    CLOG_LOG(ERROR, "serialization::encode_i32 failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(data_ + offset, AGGRE_LOG_RESERVED_SIZE, pos, submit_timestamp))) {
    CLOG_LOG(ERROR, "serialization::encode_i64 failed", K(ret));
  }
  MEMCPY(data_ + offset + AGGRE_LOG_RESERVED_SIZE, data, data_size);
  inc_update(max_submit_timestamp_, submit_timestamp);
  add_callback_to_list_(cb);
  last_access_timestamp_ = ObTimeUtility::current_time();
}

void ObAggreBuffer::wait(const uint64_t id, int64_t& wait_times)
{
  uint64_t real_id = 0;
  wait_times = 0;
  const int64_t MAX_SLEEP_US = 100;
  while ((real_id = ATOMIC_LOAD(&id_)) < id) {
    wait_times++;
    // Avoid busy waiting here
    // There will be no deadlock here, but if you use PAUSE, it may cause an avalanche
    int64_t sleep_us = wait_times * 10;
    if (sleep_us > MAX_SLEEP_US) {
      sleep_us = MAX_SLEEP_US;
    }
    usleep(sleep_us);
  }
}

void ObAggreBuffer::reuse(const uint64_t id)
{
  ref_ = 0;
  // data_ no need reset
  data_size_ = 0;
  max_submit_timestamp_ = 0;

  ObSpinLockGuard guard(lock_);
  cb_list_head_ = NULL;
  cb_list_tail_ = NULL;

  // Must put it at the end
  ATOMIC_STORE(&id_, id);
}

void ObAggreBuffer::add_callback_to_list_(ObISubmitLogCb* cb)
{
  if (NULL != cb) {
    const bool is_high_priority = cb->is_high_priority();
    ObSpinLockGuard guard(lock_);
    if (OB_ISNULL(cb_list_head_) || OB_ISNULL(cb_list_tail_)) {
      cb->next_ = NULL;
      cb_list_head_ = cb;
      cb_list_tail_ = cb;
    } else {
      if (is_high_priority) {
        cb->next_ = cb_list_head_;
        cb_list_head_ = cb;
      } else {
        cb->next_ = NULL;
        cb_list_tail_->next_ = cb;
        cb_list_tail_ = cb;
      }
    }
  }
}
}  // namespace clog
}  // namespace oceanbase
