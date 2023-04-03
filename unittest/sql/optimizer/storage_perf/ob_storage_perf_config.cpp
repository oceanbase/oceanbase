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

#include "ob_storage_perf_config.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;
namespace storageperf
{

QuerySpeed::QuerySpeed(uint64_t avg_speed, uint64_t row_cache_speed,
                       uint64_t block_cache_speed, uint64_t read_one_micro_block_speed,
                       uint64_t read_two_micro_block_speed)
  : avg_speed_(avg_speed)
  , row_cache_speed_(row_cache_speed)
  , block_cache_speed_(block_cache_speed)
  , read_one_micro_block_speed_(read_one_micro_block_speed)
  , read_two_micro_block_speed_(read_two_micro_block_speed)
{

}

bool QuerySpeed::is_over(QuerySpeed *query_speed) {
  bool ret = false;
  if(NULL == query_speed) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is NULL");
  } else if(llabs(avg_speed_ - query_speed->avg_speed_) > query_speed_over_limit_percentage
            || llabs(row_cache_speed_ - query_speed->row_cache_speed_) > query_speed_over_limit_percentage
            || llabs(block_cache_speed_ - query_speed->block_cache_speed_) > query_speed_over_limit_percentage
            || llabs(read_one_micro_block_speed_ - query_speed->read_one_micro_block_speed_) > query_speed_over_limit_percentage
            || llabs(read_two_micro_block_speed_ - query_speed->read_two_micro_block_speed_) > query_speed_over_limit_percentage){
    ret = true;
  }
  return ret;
}

int64_t QuerySpeed::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos,
                  "avg_speed = %lu  row_cache_speed = %lu block_cache_speed = %lu  "
                  "read_one_micro_block_speed = %lu  read_two_micro_block_speed = %lu "
                  "query_speed_over_limit_percentage = %lu",
                  avg_speed_, row_cache_speed_, block_cache_speed_, read_one_micro_block_speed_,
                  read_two_micro_block_speed_, query_speed_over_limit_percentage);
  return pos;
}

ObStoragePerfConfig::ObStoragePerfConfig()
  : is_inited_(false)
  , single_get_speed_(0, 0, 0, 0, 0)
  , multi_get_speed_(0, 0, 0, 0, 0)
  , scan_speed_(0, 0, 0, 0, 0), print_row_(false), print_perf_stat_(false), read_columns_count_(-1),
  flush_block_index_cache_(false),
    flush_block_cache_(false),
    flush_row_cache_(false),
    flush_bf_cache_(false),
    log_level_(NULL), get_range_(1), unittest_mode_(true)
{
}

ObStoragePerfConfig::~ObStoragePerfConfig()
{
}

int ObStoragePerfConfig::init(const char *config_file)
{
  int ret = OB_SUCCESS;
  obsys::ObSysConfig config;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice");
  } else if (NULL == config_file) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "config file is NULL", K(config_file));
  } else if (OB_FAIL(config.load(config_file))) {
    STORAGE_LOG(WARN, "failed to load config", K(config_file), K(ret));
  } else {
    const char *perf_root_dir = config.getString("common_parameter", "perf_root_dir", "");
    int n1 = snprintf(perf_root_dir_, MAX_PATH_SIZE, "%s", perf_root_dir);
    int n2 = snprintf(slog_dir_, MAX_PATH_SIZE, "%s/slog", perf_root_dir);
    int n3 = snprintf(sstable_meta_dir_, MAX_PATH_SIZE, "%s/sstable_meta", perf_root_dir);
    int n4 = snprintf(sstable_data_dir_, MAX_PATH_SIZE, "%s/sstable", perf_root_dir);
    int n5 = snprintf(perf_log_dir_, MAX_PATH_SIZE, "%s/log", perf_root_dir);
    if (n1 < 0 || n1 > MAX_PATH_SIZE
        || n2 < 0 || n2 > MAX_PATH_SIZE
        || n3 < 0 || n3 > MAX_PATH_SIZE
        || n4 < 0 || n4 > MAX_PATH_SIZE
        || n5 < 0 || n5 > MAX_PATH_SIZE) {
        ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buf not enough", K(perf_root_dir), K(n1), K(n2), K(n3), K(n4), K(n5));
    }

    partition_size_ = config.getInt("common_parameter", "partition_size");
    tenant_cache_size_ = config.getInt("common_parameter", "tenant_cache_size");
    scan_use_cache_ = config.getInt("common_parameter", "scan_use_cache");
    print_row_info_ = config.getInt("common_parameter", "print_row_info");
    test_single_row_speed_ = config.getInt("common_parameter", "test_single_row_speed");
    io_submit_thread_num_ = config.getInt("common_parameter", "io_submit_thread_num");
    io_queue_depth_ = config.getInt("common_parameter", "io_queue_depth");
    total_partition_num_ = config.getInt("common_parameter", "total_partition_num");
    is_test_write_ = config.getInt("write_parameter", "is_test_write");
    write_to_memtable_percent_ = config.getInt("write_parameter", "write_to_memtable_percent");
    single_get_thread_count_ = config.getInt("read_parameter", "single_get_thread_count");
    multi_get_thread_count_ = config.getInt("read_parameter", "multi_get_thread_count");
    multi_get_run_ = config.getInt("read_parameter", "multi_get_run");
    scan_thread_count_ = config.getInt("read_parameter", "scan_thread_count");
    total_single_row_count_ = config.getInt("read_parameter", "total_single_row_count");
    single_get_times_ = config.getInt("read_parameter", "single_get_times");
    total_multi_row_count_ = config.getInt("read_parameter", "total_multi_row_count");
    multi_get_times_ = config.getInt("read_parameter", "multi_get_times");
    total_scan_row_count_ = config.getInt("read_parameter", "total_scan_row_count");
    scan_times_ = config.getInt("read_parameter", "scan_times");
    scan_run_ = config.getInt("read_parameter", "scan_run");
    print_row_ = config.getInt("read_parameter", "print_row");
    //get base single get speed
    single_get_speed_.avg_speed_ = config.getInt("single_get_speed", "avg_speed");
    single_get_speed_.row_cache_speed_ = config.getInt("single_get_speed", "row_cache_speed");
    single_get_speed_.block_cache_speed_ = config.getInt("single_get_speed", "block_cache_speed");
    single_get_speed_.read_one_micro_block_speed_ = config.getInt("single_get_speed", "read_one_micro_block_speed");
    single_get_speed_.read_two_micro_block_speed_ = config.getInt("single_get_speed", "read_two_micro_block_speed");
    //get base multi get speed
    multi_get_speed_.avg_speed_ = config.getInt("multi_get_speed", "avg_speed");
    multi_get_speed_.row_cache_speed_ = config.getInt("multi_get_speed", "row_cache_speed");
    multi_get_speed_.block_cache_speed_ = config.getInt("multi_get_speed", "block_cache_speed");
    multi_get_speed_.read_one_micro_block_speed_ = config.getInt("multi_get_speed", "read_one_micro_block_speed");
    multi_get_speed_.read_two_micro_block_speed_ = config.getInt("multi_get_speed", "read_two_micro_block_speed");
    //get scan speed
    scan_speed_.avg_speed_ = config.getInt("scan_speed", "avg_speed");
    scan_speed_.row_cache_speed_ = config.getInt("scan_speed", "row_cache_speed");
    scan_speed_.block_cache_speed_ = config.getInt("scan_speed", "block_cache_speed");
    scan_speed_.read_one_micro_block_speed_ = config.getInt("scan_speed", "read_one_micro_block_speed");
    scan_speed_.read_two_micro_block_speed_ = config.getInt("scan_speed", "read_two_micro_block_speed");

  }
  return ret;
}

} /* namespace storagetest */
} /* namespace oceanbase */
