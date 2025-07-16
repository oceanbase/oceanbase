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

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_tmp_file_write_cache_metrics.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace tmp_file
{

void ObTmpFileFlushMetrics::reset()
{
  // Do not reset real_time_flushing_page_cnt_
  ATOMIC_SET(&flush_range_cnt_, 0);
  ATOMIC_SET(&total_page_cnt_, 0);
  ATOMIC_SET(&max_page_cnt_, -1);
  ATOMIC_SET(&min_page_cnt_, INT64_MAX);
  ATOMIC_SET(&skip_incomplete_page_cnt_, 0);
  ATOMIC_SET(&incomplete_page_flush_cnt_, 0);
  ATOMIC_SET(&meta_page_cnt_, 0);
}

void ObTmpFileFlushMetrics::print_statistics()
{
  int64_t total_page_cnt = ATOMIC_LOAD(&total_page_cnt_);
  int64_t flush_data_MB = total_page_cnt * ObTmpFileGlobal::PAGE_SIZE / 1024 / 1024;
  int64_t flush_range_cnt = ATOMIC_LOAD(&flush_range_cnt_);
  int64_t avg_flush_page_cnt = ATOMIC_LOAD(&total_page_cnt_) / max(flush_range_cnt, 1);
  int64_t max_flush_page_cnt = ATOMIC_LOAD(&max_page_cnt_);
  int64_t min_flush_page_cnt = ATOMIC_LOAD(&min_page_cnt_) == INT64_MAX ? -1 : min_page_cnt_;
  int64_t meta_page_cnt = ATOMIC_LOAD(&meta_page_cnt_);
  int64_t skip_incomplete_page_cnt = ATOMIC_LOAD(&skip_incomplete_page_cnt_);
  int64_t incomplete_page_flush_cnt = ATOMIC_LOAD(&incomplete_page_flush_cnt_);
  int64_t real_time_flushing_page_cnt = ATOMIC_LOAD(&real_time_flushing_page_cnt_);
  LOG_INFO("tmp file flush statistics",
      K(flush_data_MB), K(total_page_cnt),
      K(flush_range_cnt), K(avg_flush_page_cnt),
      K(max_flush_page_cnt), K(min_flush_page_cnt),
      K(meta_page_cnt),
      K(skip_incomplete_page_cnt), K(incomplete_page_flush_cnt),
      K(real_time_flushing_page_cnt));
  reset();
}

void ObTmpFileFlushMetrics::record_flush_task(const int64_t page_num)
{
  ATOMIC_INC(&flush_range_cnt_);
  ATOMIC_AAF(&total_page_cnt_, page_num);
  int max_page_cnt = ATOMIC_LOAD(&max_page_cnt_);
  int min_page_cnt = ATOMIC_LOAD(&min_page_cnt_);
  ATOMIC_SET(&max_page_cnt_, max(max_page_cnt, page_num));
  ATOMIC_SET(&min_page_cnt_, min(min_page_cnt, page_num));
}

void ObTmpFileFlushMetrics::record_skip_incomplete_page(const int64_t page_num)
{
  ATOMIC_AAF(&flush_range_cnt_, page_num);
}

void ObTmpFileFlushMetrics::record_incomplete_page(const int64_t page_num)
{
  ATOMIC_AAF(&incomplete_page_flush_cnt_, page_num);
}

void ObTmpFileFlushMetrics::record_meta_page(const int64_t page_num)
{
  ATOMIC_AAF(&meta_page_cnt_, page_num);
}

void ObTmpFileSwapMetrics::reset()
{
  ATOMIC_SET(&swap_task_cnt_, 0);
  ATOMIC_SET(&swap_total_response_time_, 0);
  ATOMIC_SET(&swap_max_response_time_, -1);
  ATOMIC_SET(&swap_min_response_time_, INT64_MAX);
}

void ObTmpFileSwapMetrics::print_statistics()
{
  int64_t swap_task_cnt = ATOMIC_LOAD(&swap_task_cnt_);
  int64_t avg_swap_response_time = ATOMIC_LOAD(&swap_total_response_time_) / max(swap_task_cnt, 1);
  int64_t max_swap_response_time = ATOMIC_LOAD(&swap_max_response_time_);
  int64_t min_swap_response_time = ATOMIC_LOAD(&swap_min_response_time_) == INT64_MAX ? -1 : swap_min_response_time_;
  LOG_INFO("tmp file swap statistics",
      K(swap_task_cnt), K(avg_swap_response_time),
      K(max_swap_response_time), K(min_swap_response_time));
  reset();
}

void ObTmpFileSwapMetrics::record_swap_response_time(const int64_t response_time)
{
  ATOMIC_INC(&swap_task_cnt_);
  ATOMIC_AAF(&swap_total_response_time_, response_time);
  int64_t max_response_time = ATOMIC_LOAD(&swap_max_response_time_);
  int64_t min_response_time = ATOMIC_LOAD(&swap_min_response_time_);
  ATOMIC_SET(&swap_max_response_time_, max(max_response_time, response_time));
  ATOMIC_SET(&swap_min_response_time_, min(min_response_time, response_time));
}

void ObTmpFileWriteCacheMetrics::print()
{
  swap_metrics_.print_statistics();
  flush_metrics_.print_statistics();
}

void ObTmpFileWriteCacheMetrics::record_flush_task(const int64_t page_num)
{
  ATOMIC_AAF(&flush_metrics_.real_time_flushing_page_cnt_, page_num);
  flush_metrics_.record_flush_task(page_num);
}

void ObTmpFileWriteCacheMetrics::record_swap_job(const int64_t response_time)
{
  swap_metrics_.record_swap_response_time(response_time);
}

void ObTmpFileWriteCacheMetrics::record_flush_task_over(const int64_t page_num)
{
  ATOMIC_AAF(&flush_metrics_.real_time_flushing_page_cnt_, -page_num);
}

void ObTmpFileWriteCacheMetrics::record_meta_page(const int64_t page_num)
{
  flush_metrics_.record_meta_page(page_num);
}

void ObTmpFileWriteCacheMetrics::record_skip_incomplete_page(const int64_t page_num)
{
  flush_metrics_.record_skip_incomplete_page(page_num);
}

void ObTmpFileWriteCacheMetrics::record_incomplete_page(const int64_t page_num)
{
  flush_metrics_.record_incomplete_page(page_num);
}

}  // end namespace tmp_file
}  // end namespace oceanbase
