/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_BASIC_OB_LAKE_TABLE_READER_PROFILE_H_
#define OCEANBASE_BASIC_OB_LAKE_TABLE_READER_PROFILE_H_

#include "lib/container/ob_se_array.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
namespace sql
{

struct ObLakeTableIMetrics
{
  ObLakeTableIMetrics() : label_()
  {}
  virtual int update_profile() = 0;
  virtual void dump_metrics() = 0;
  void set_label(const common::ObString &label);
  VIRTUAL_TO_STRING_KV(K(this));

  common::ObString label_;
};

struct ObLakeTableReaderMetrics : public ObLakeTableIMetrics
{
public:
  ObLakeTableReaderMetrics() :
    selected_file_count_(0), skipped_file_count_(0), selected_page_count_(0),
    skipped_page_count_(0), selected_row_group_count_(0), skipped_row_group_count_(0),
    read_rows_count_()
  {}
  virtual int update_profile() override;
  virtual void dump_metrics() override;
  VIRTUAL_TO_STRING_KV(K_(selected_file_count), K_(skipped_file_count), K_(selected_page_count),
                       K_(skipped_page_count), K_(selected_row_group_count),
                       K_(skipped_row_group_count), K_(read_rows_count));

  int64_t selected_file_count_;
  int64_t skipped_file_count_;
  int64_t selected_page_count_;
  int64_t skipped_page_count_;
  int64_t selected_row_group_count_;
  int64_t skipped_row_group_count_;
  int64_t read_rows_count_;
};

struct ObLakeTablePreBufferMetrics : public ObLakeTableIMetrics
{
public:
  ObLakeTablePreBufferMetrics() :
    prebuffer_count_(0), miss_count_(0), hit_count_(0), async_io_count_(0),
    async_io_size_(0), total_io_wait_time_us_(0), max_io_wait_time_us_(0),
    total_read_size_(0)
  {}
  virtual int update_profile() override;
  virtual void dump_metrics() override;
  VIRTUAL_TO_STRING_KV(K_(prebuffer_count), K_(miss_count), K_(hit_count),
                       K_(async_io_count), K_(async_io_size), K_(total_io_wait_time_us),
                       K_(max_io_wait_time_us), K_(total_read_size));

  int64_t prebuffer_count_; // number of prebuffer calls
  int64_t miss_count_;
  int64_t hit_count_;
  int64_t async_io_count_; // number of async read calls
  int64_t async_io_size_; // total size of async read IO
  int64_t total_io_wait_time_us_; // total waiting time for async read IO
  int64_t max_io_wait_time_us_; // max waiting time for async read IO
  int64_t total_read_size_; // total size read from the prebuffer
};

struct ObLakeTableIOMetrics : public ObLakeTableIMetrics
{
public:
  ObLakeTableIOMetrics() :
    access_count_(0), sync_read_count_(0), async_read_count_(0), io_count_(0), access_io_size_(0),
    mem_cache_hit_count_(0), mem_cache_hit_io_size_(0), mem_cache_miss_count_(0),
    mem_cache_miss_io_size_(0), disk_cache_hit_count_(0), disk_cache_hit_io_size_(0),
    disk_cache_miss_count_(0), disk_cache_miss_io_size_(0), max_io_time_us_(0), total_io_time_us_(0)
  {}
  virtual int update_profile() override;
  virtual void dump_metrics() override;
  OB_INLINE void update_disk_cache_stat(const bool is_hit, const int64_t io_size)
  {
    if (is_hit) {
      ++disk_cache_hit_count_;
      disk_cache_hit_io_size_ += io_size;
    } else {
      ++disk_cache_miss_count_;
      disk_cache_miss_io_size_ += io_size;
    }
  }
  OB_INLINE void update_mem_cache_stat(const bool is_hit, const int64_t io_size)
  {
    if (is_hit) {
      ++mem_cache_hit_count_;
      mem_cache_hit_io_size_ += io_size;
      EVENT_ADD(ObStatEventIds::EXTERNAL_TABLE_MEM_CACHE_HIT_BYTES, io_size);
    } else {
      ++mem_cache_miss_count_;
      mem_cache_miss_io_size_ += io_size;
      EVENT_ADD(ObStatEventIds::EXTERNAL_TABLE_MEM_CACHE_MISS_BYTES, io_size);
    }
  }
  OB_INLINE void update_io_stat(const int64_t io_time_us)
  {
    ++io_count_;
    total_io_time_us_ += io_time_us;
    max_io_time_us_ = MAX(max_io_time_us_, io_time_us);
    EVENT_ADD(ObStatEventIds::EXTERNAL_TABLE_IO_TIME, io_time_us);
  }
  OB_INLINE void update_access_stat(const bool is_async, const int64_t read_size)
  {
    ++access_count_;
    access_io_size_ += read_size;
    if (is_async) {
      ++async_read_count_;
    } else {
      ++sync_read_count_;
    }
    EVENT_INC(ObStatEventIds::EXTERNAL_TABLE_IO_CNT);
    EVENT_ADD(ObStatEventIds::EXTERNAL_TABLE_IO_SIZE, read_size);
  }
  VIRTUAL_TO_STRING_KV(K_(access_count), K_(sync_read_count), K_(async_read_count), K_(io_count),
                       K_(access_io_size), K_(mem_cache_hit_count), K_(mem_cache_hit_io_size),
                       K_(mem_cache_miss_count), K_(mem_cache_miss_io_size),
                       K_(disk_cache_hit_count), K_(disk_cache_hit_io_size),
                       K_(disk_cache_miss_count), K_(disk_cache_miss_io_size), K_(max_io_time_us),
                       "avg_io_time_us", (io_count_ > 0 ? total_io_time_us_ / io_count_ : 0),
                       K_(total_io_time_us));

  // SQL file access layer
  int64_t access_count_; // number of read calls made by the SQL layer
  int64_t sync_read_count_;
  int64_t async_read_count_;
  int64_t io_count_; // number of IOs sent to the storage layer
  int64_t access_io_size_; // total size of read IO

  // Memory Cache Layer
  int64_t mem_cache_hit_count_;
  int64_t mem_cache_hit_io_size_; // total IO size that hits the memory cache
  int64_t mem_cache_miss_count_;
  int64_t mem_cache_miss_io_size_; // total IO size that hits the memory cache

  // Disk Cache Layer
  int64_t disk_cache_hit_count_;
  int64_t disk_cache_hit_io_size_; // total IO size that hits the disk cache
  int64_t disk_cache_miss_count_;
  int64_t disk_cache_miss_io_size_; // total IO size that miss the disk cache
  int64_t max_io_time_us_; // maximum waiting time for read IO
  int64_t total_io_time_us_; // total waiting time for read IO
};

class ObLakeTableReaderProfile
{
public:
  ObLakeTableReaderProfile() : metrics_list_()
  {}
  int update_profile();
  int register_metrics(ObLakeTableIMetrics *metrics, const ObString &label);
  void dump_metrics();

private:
  typedef common::ObSEArray<ObLakeTableIMetrics*, 8> MetricsList;
  MetricsList metrics_list_;
};


} // end of sql namespace
} // end of oceanbase namespace

#endif // OCEANBASE_BASIC_OB_LAKE_TABLE_READER_PROFILE_H_