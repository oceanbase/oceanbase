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

#ifndef OCEANBASE_CLOG_OB_MAX_LOG_META_INFO_H_
#define OCEANBASE_CLOG_OB_MAX_LOG_META_INFO_H_

#include "ob_log_define.h"
#include "ob_i_submit_log_cb.h"
#include "lib/atomic/atomic128.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase {
namespace clog {
class ObMaxLogMetaInfo {
public:
  ObMaxLogMetaInfo();
  ~ObMaxLogMetaInfo();

public:
  int init(const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t timestamp);
  void reset();
  uint64_t get_log_id() const;
  int64_t get_timestamp() const;
  int get_log_id_and_timestamp(uint64_t& log_id, int64_t& timestamp) const;
  int alloc_log_id_ts(
      const int64_t base_timestamp, const uint64_t log_id_limit, uint64_t& log_id, int64_t& timestamp, int64_t& offset);
  int alloc_log_id_ts(const int64_t base_timestamp, const int64_t size, const uint64_t log_id_limit, uint64_t& log_id,
      int64_t& timestamp, int64_t& offset);
  int try_freeze(const uint64_t log_id, int64_t& offset);
  int try_update_timestamp(const int64_t base_timestamp);
  int try_update_log_id(const uint64_t log_id);

private:
  void check_log_id_range_(const uint64_t log_id) const;
  // a.v128_.lo = ((log_id & MASK) << 16) + offset;
  // a.v128_.hi = ts;
  const static int64_t MASK = 0xffffffffffff;
  const static int64_t LOG_ID_LIMIT = MASK - 100000;
  const static int64_t LOG_ID_CHECK_RANGE = MASK / 2;
  union MetaInfo {
    struct types::uint128_t v128_;
    struct {
      uint64_t offset_ : 16;
      uint64_t log_id_ : 48;
      int64_t ts_ : 64;
    } __attribute__((__aligned__(16)));
  };

private:
  common::ObPartitionKey partition_key_;
  MetaInfo meta_info_;
  int64_t saved_last_gts_;
  int64_t saved_last_get_gts_ts_;

  int64_t total_log_cnt_;
  int64_t total_commit_log_cnt_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMaxLogMetaInfo);
};

class ObAggreBuffer {
public:
  ObAggreBuffer();
  ~ObAggreBuffer();

public:
  int init(const uint64_t id, const uint64_t tenant_id);
  void reset();
  void destroy();

  const char* get_data() const;
  int64_t get_data_size() const;
  int64_t get_max_submit_timestamp() const;
  ObISubmitLogCb* get_submit_cb() const;

  int64_t ref(const int64_t delta);
  void fill(const int64_t offset, const char* data, const int64_t data_size, const int64_t submit_timestamp,
      ObISubmitLogCb* cb);
  void wait(const uint64_t id, int64_t& wait_times);
  void reuse(const uint64_t id);

private:
  void add_callback_to_list_(ObISubmitLogCb* cb);

private:
  uint64_t id_;
  int64_t ref_;
  char* data_;
  int64_t data_size_;
  int64_t max_submit_timestamp_;
  int64_t last_access_timestamp_;

  ObISubmitLogCb* cb_list_head_;
  ObISubmitLogCb* cb_list_tail_;
  // lock for cb_list_head_ and cb_list_tail_
  common::ObSpinLock lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAggreBuffer);
} CACHE_ALIGNED;
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_MAX_LOG_META_INFO_H_
