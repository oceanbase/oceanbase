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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_METRICS_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_METRICS_H_

#include "lib/utility/utility.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace tmp_file
{

struct ObTmpFileFlushMetrics
{
public:
  ObTmpFileFlushMetrics()
    : real_time_flushing_page_cnt_(0),
      flush_range_cnt_(0),
      total_page_cnt_(0),
      max_page_cnt_(-1),
      min_page_cnt_(INT64_MAX),
      skip_incomplete_page_cnt_(0),
      incomplete_page_flush_cnt_(0),
      meta_page_cnt_(0) {}
  void reset();
  // reset statistical counters after reporting
  void print_statistics();
  void record_flush_task(const int64_t page_num);
  void record_skip_incomplete_page(const int64_t page_num);
  void record_incomplete_page(const int64_t page_num);
  void record_meta_page(const int64_t page_num);
public:
  int64_t real_time_flushing_page_cnt_;
  int64_t flush_range_cnt_;
  int64_t total_page_cnt_;
  int64_t max_page_cnt_;
  int64_t min_page_cnt_;
  int64_t skip_incomplete_page_cnt_;
  int64_t incomplete_page_flush_cnt_;
  int64_t meta_page_cnt_;
};

struct ObTmpFileSwapMetrics
{
public:
  ObTmpFileSwapMetrics()
    : swap_task_cnt_(0),
      swap_total_response_time_(0),
      swap_max_response_time_(-1),
      swap_min_response_time_(INT64_MAX) {}
  void reset();
  // reset statistical counters after reporting
  void print_statistics();
  void record_swap_response_time(const int64_t response_time);
public:
  int64_t swap_task_cnt_;
  int64_t swap_total_response_time_;
  int64_t swap_max_response_time_;
  int64_t swap_min_response_time_;
};

struct ObTmpFileWriteCacheMetrics
{
public:
  void print();
  void record_flush_task(const int64_t page_num);
  void record_swap_job(const int64_t response_time);
  void record_flush_task_over(const int64_t page_num);
  void record_meta_page(const int64_t page_num);
  void record_skip_incomplete_page(const int64_t page_num);
  void record_incomplete_page(const int64_t page_num);
  OB_INLINE int64_t get_flushing_data_size()
  {
    return flush_metrics_.real_time_flushing_page_cnt_ * ObTmpFileGlobal::PAGE_SIZE;
  }
public:
  ObTmpFileFlushMetrics flush_metrics_;
  ObTmpFileSwapMetrics swap_metrics_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_THREAD_JOB_H_
