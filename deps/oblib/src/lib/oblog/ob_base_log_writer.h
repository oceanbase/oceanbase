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

#ifndef OB_BASE_LOG_WRITER_H_
#define OB_BASE_LOG_WRITER_H_
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

namespace oceanbase {
namespace common {
class ObIBaseLogItem {
public:
  ObIBaseLogItem()
  {}
  virtual ~ObIBaseLogItem()
  {}
  virtual char* get_buf() = 0;
  virtual const char* get_buf() const = 0;
  virtual int64_t get_data_len() const = 0;
};

struct ObBaseLogWriterCfg {
  ObBaseLogWriterCfg()
      : max_buffer_item_cnt_(DEFAULT_MAX_BUFFER_ITEM_CNT),
        group_commit_max_wait_us_(0),
        group_commit_min_item_cnt_(0),
        group_commit_max_item_cnt_(0)
  {}
  ObBaseLogWriterCfg(const uint64_t max_buffer_item_cnt, const uint64_t group_commit_max_wait_us,
      const uint64_t group_commit_min_item_cnt, const uint64_t group_commit_max_item_cnt)
      : max_buffer_item_cnt_(max_buffer_item_cnt),
        group_commit_max_wait_us_(group_commit_max_wait_us),
        group_commit_min_item_cnt_(group_commit_min_item_cnt),
        group_commit_max_item_cnt_(group_commit_max_item_cnt)
  {}
  virtual ~ObBaseLogWriterCfg()
  {}
  virtual inline bool is_valid() const
  {
    return max_buffer_item_cnt_ > 0 && group_commit_min_item_cnt_ > 0 &&
           group_commit_min_item_cnt_ <= max_buffer_item_cnt_ && group_commit_max_item_cnt_ > 0 &&
           group_commit_min_item_cnt_ <= group_commit_max_item_cnt_;
  }
  static const uint64_t DEFAULT_MAX_BUFFER_ITEM_CNT = 1024;
  uint64_t max_buffer_item_cnt_;
  uint64_t group_commit_max_wait_us_;
  uint64_t group_commit_min_item_cnt_;
  uint64_t group_commit_max_item_cnt_;
};

class ObBaseLogWriter {
public:
  ObBaseLogWriter();
  virtual ~ObBaseLogWriter();
  virtual int init(const ObBaseLogWriterCfg& log_cfg);
  virtual void destroy();

  int append_log(ObIBaseLogItem& log_item, const uint64_t timeout_us = DEFAULT_LOG_APPEND_TIMEOUT_US);
  int reconfig(const ObBaseLogWriterCfg& log_cfg);
  void wait();
  int64_t get_flush_tid() const
  {
    return flush_tid_;
  }
  bool is_inited() const
  {
    return is_inited_;
  }
  bool has_stoped() const
  {
    return has_stoped_;
  }
  void set_stoped()
  {
    has_stoped_ = true;
  }
  virtual int64_t get_queued_item_cnt() const
  {
    return log_item_push_idx_ - log_item_pop_idx_;
  }

protected:
  virtual void process_log_items(ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt) = 0;

private:
  static void* flush_log_thread(void* arg);

  void flush_log();
  bool need_flush();

private:
  static const uint64_t DEFAULT_LOG_APPEND_TIMEOUT_US = 100;
  static const uint64_t MAX_STOP_WAIT_TIME_US = 1000000;
  bool is_inited_;
  bool has_stoped_;
  ObBaseLogWriterCfg log_cfg_;
  pthread_t flush_tid_;

protected:
  // async log queue
  ObIBaseLogItem** log_items_;
  ObIBaseLogItem** process_items_;
  uint64_t max_buffer_item_cnt_;
  int64_t log_item_push_idx_;
  int64_t log_item_pop_idx_;

  pthread_mutex_t log_mutex_;
  pthread_cond_t log_write_cond_;
  pthread_cond_t log_flush_cond_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* OB_BASE_LOG_WRITER_H_ */
