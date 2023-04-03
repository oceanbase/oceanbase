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

#ifndef TOOLS_STORAGE_PERF_TEST_CONFIG_H_
#define TOOLS_STORAGE_PERF_TEST_CONFIG_H_

#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_define.h"
#include <cstdio>
namespace oceanbase
{
using namespace common;
namespace storageperf
{

static const int64_t rowkey_column_count = 23;
static const int64_t column_num = rowkey_column_count + 23;
static const int64_t update_column_num = rowkey_column_count + 7;
static const int64_t data_version = 1;
static const int64_t tenant_id = 1;
static const int64_t pure_id = 3001;
static const int64_t row_count_per_macro = 2900;//row count in one macro block
static const int64_t query_timeout = 10L * 3600L * 1000L * 1000L;//10 hour
static const int64_t query_speed_over_limit_percentage = 10;
static const int64_t update_rows_num = 1000;
static const common::ObString bianque_format("%Y-%m-%dT%H:%i:%sZ");
static const int64_t max_time_string_length = 20;


struct QuerySpeed {
  QuerySpeed(uint64_t avg_speed, uint64_t row_cache_speed,
             uint64_t block_cache_speed, uint64_t read_one_micro_block_speed,
             uint64_t read_two_micro_block_speed);
  uint64_t avg_speed_;
  uint64_t row_cache_speed_;
  uint64_t block_cache_speed_;
  uint64_t read_one_micro_block_speed_;
  uint64_t read_two_micro_block_speed_;
  bool is_over(QuerySpeed *query_speed);
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

class ObStoragePerfConfig
{
public:
  ObStoragePerfConfig();
  virtual ~ObStoragePerfConfig();
  int init(const char *config_file);
  int64_t get_partition_size() { return partition_size_; }
  const char *get_perf_root_dir() { return perf_root_dir_; }
  const char *get_perf_log_dir() { return perf_log_dir_; }
  const char *get_sstable_data_dir() { return sstable_data_dir_; }
  const char *get_slog_dir() { return slog_dir_; }
  const char *get_sstable_meta_dir() { return sstable_meta_dir_; }
  int64_t get_total_single_row_count() { return total_single_row_count_; }
  int64_t get_total_multi_row_count() { return total_multi_row_count_; }
  int64_t get_total_scan_row_count() { return total_scan_row_count_; }
  int get_single_get_thread_count() { return single_get_thread_count_; }
  int get_multi_get_thread_count() { return multi_get_thread_count_; }
  int get_multi_get_run() {return multi_get_run_;}
  int get_scan_thread_count() { return scan_thread_count_; }
  bool is_test_write() { return (is_test_write_ > 0) ? true : false; }
  int get_write_to_memtable_percent() {return write_to_memtable_percent_;}
  int64_t get_tenant_cache_size() { return tenant_cache_size_; }
  int get_print_row_info() { return print_row_info_; }
  int is_test_single_row_speed() { return test_single_row_speed_; }
  int get_scan_use_cache() { return scan_use_cache_; }
  int get_total_partition_num() { return total_partition_num_; }
  int32_t get_io_submit_thread_num() {return io_submit_thread_num_;}
  int32_t get_io_queue_depth() {return io_queue_depth_;}
  int64_t get_single_get_times() {return single_get_times_;}
  int64_t get_multi_get_times() {return multi_get_times_;}
  int64_t get_scan_times() {return scan_times_;}
  int get_scan_run() {return scan_run_;}
  QuerySpeed &get_single_get_speed() { return single_get_speed_; }
  QuerySpeed &get_multi_get_speed()  { return multi_get_speed_; }
  static int64_t print_bianque_timestamp(const char *prefix, FILE *fp)
  {
    int64_t ret = OB_SUCCESS;
    const int64_t cur_time = ObTimeUtility::current_time();
    int64_t pos = 0;
    char time_string_buf[max_time_string_length+1];
    if (OB_FAIL(ObTimeUtility::usec_format_to_str(cur_time, bianque_format, &time_string_buf[0], max_time_string_length, pos))) {
      STORAGE_LOG(WARN, "failed to convert bianque format string", K(ret));
    } else {
      time_string_buf[pos] = '\0';
      fprintf(fp, "PerfStat: %s: %s\n", prefix, time_string_buf);
    }
    return ret;
  }
  QuerySpeed &get_scan_speed() { return scan_speed_; }
//private:
  int64_t partition_size_;
  char perf_root_dir_[common::MAX_PATH_SIZE];
  char perf_log_dir_[common::MAX_PATH_SIZE];
  char sstable_data_dir_[common::MAX_PATH_SIZE];
  char slog_dir_[common::MAX_PATH_SIZE];
  char sstable_meta_dir_[common::MAX_PATH_SIZE];
  int64_t total_single_row_count_;
  int64_t total_multi_row_count_;
  int64_t total_scan_row_count_;
  int32_t io_submit_thread_num_;
  int32_t io_queue_depth_;
  int64_t single_get_times_;
  int64_t multi_get_times_;
  int64_t scan_times_;
  int scan_run_;
  int single_get_thread_count_;
  int multi_get_thread_count_;
  int scan_thread_count_;
  int is_test_write_;
  int write_to_memtable_percent_;
  int64_t tenant_cache_size_;
  int print_row_info_;
  int test_single_row_speed_;
  int scan_use_cache_;
  int multi_get_run_;
  bool is_inited_;
  QuerySpeed single_get_speed_;
  QuerySpeed multi_get_speed_;
  QuerySpeed scan_speed_;
  int total_partition_num_;

  bool print_row_;
  bool print_perf_stat_;
  int64_t read_columns_count_;

  bool flush_block_index_cache_;
  bool flush_block_cache_;
  bool flush_row_cache_;
  bool flush_bf_cache_;
  const char *log_level_;
  int64_t get_range_;
  bool unittest_mode_;

};

} /* namespace storagetest */
} /* namespace oceanbase */

#endif /* TOOLS_STORAGE_PERF_TEST_H_ */
