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
#include <cstdint>                                       // UINT64_MAX
#include "lib/ob_errno.h"                                // errno
#include "lib/utility/ob_print_utils.h"                  // TO_STRING_KV
#include "log_define.h"
#include "lsn.h"
namespace oceanbase
{
namespace palf
{
class LogIteratorInfo
{
public:
  LogIteratorInfo()
      : allow_filling_cache_(true), hot_cache_stat_(), cold_cache_stat_(),
        read_io_cnt_(0), read_io_size_(0), read_disk_cost_ts_(0) {}
  LogIteratorInfo(bool allow_filling_cache)
      : allow_filling_cache_(allow_filling_cache), hot_cache_stat_(), cold_cache_stat_(),
        read_io_cnt_(0), read_io_size_(0), read_disk_cost_ts_(0) {}
  ~LogIteratorInfo() {
    reset();
  }
  void reset() {
    allow_filling_cache_ = false;
    hot_cache_stat_.reset();
    cold_cache_stat_.reset();
    read_io_cnt_ = 0;
    read_io_size_ = 0;
    read_disk_cost_ts_ = 0;
  }
  bool get_allow_filling_cache() const {
    return allow_filling_cache_;
  }
  void set_allow_filling_cache(const bool allow_filling_cache) {
    allow_filling_cache_ = allow_filling_cache;
  }
  void inc_hit_cnt(const bool is_cold_cache = true) {
    if (is_cold_cache) {
      cold_cache_stat_.inc_hit_cnt();
    } else {
      hot_cache_stat_.inc_hit_cnt();
    }
  }
  void inc_miss_cnt(const bool is_cold_cache = true) {
    if (is_cold_cache) {
      cold_cache_stat_.inc_miss_cnt();
    } else {
      hot_cache_stat_.inc_miss_cnt();
    }
  }
  void inc_cache_read_size(int64_t cache_read_size, const bool is_cold_cache = true) {
    if (is_cold_cache) {
      cold_cache_stat_.inc_cache_read_size(cache_read_size);
    } else {
      hot_cache_stat_.inc_cache_read_size(cache_read_size);
    }
  }
  void inc_read_io_cnt() { read_io_cnt_++; }
  void inc_read_io_size(int64_t read_io_size) { read_io_size_ += read_io_size; }
  void inc_read_disk_cost_ts(int64_t read_disk_cost_ts) { read_disk_cost_ts_ += read_disk_cost_ts; }
  void set_start_lsn(const LSN &start_lsn) { start_lsn_ = start_lsn; }
  TO_STRING_KV(K_(allow_filling_cache), K_(hot_cache_stat), K_(cold_cache_stat),
               K_(read_io_cnt), K_(read_io_size), K_(read_disk_cost_ts), K_(start_lsn));

private:
  class IteratorCacheStat
  {
  public:
    IteratorCacheStat() : hit_cnt_(0), miss_cnt_(0), cache_read_size_(0) {}
    ~IteratorCacheStat() { reset(); }
    void reset() {
      hit_cnt_ = 0;
      miss_cnt_ = 0;
      cache_read_size_ = 0;
    }
    void inc_hit_cnt() { hit_cnt_++; }
    void inc_miss_cnt() { miss_cnt_++; }
    void inc_cache_read_size(int64_t cache_read_size) { cache_read_size_ += cache_read_size; }
    double get_hit_ratio() const
    {
      int64_t total_cnt = (hit_cnt_ + miss_cnt_ == 0) ? 1 : hit_cnt_ + miss_cnt_;
      return hit_cnt_ * 1.0 /total_cnt;
    }
    TO_STRING_KV(K_(hit_cnt), K_(miss_cnt), K_(cache_read_size), "hit_ratio", get_hit_ratio());
  private:
    int64_t hit_cnt_;
    int64_t miss_cnt_;
    int64_t cache_read_size_;
  };
private:
  bool allow_filling_cache_;
  IteratorCacheStat hot_cache_stat_;
  IteratorCacheStat cold_cache_stat_;
  int64_t read_io_cnt_;
  int64_t read_io_size_;
  int64_t read_disk_cost_ts_;
  LSN start_lsn_;
};
}
}