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

#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_MONITOR_H_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_MONITOR_H_
#include "share/rc/ob_tenant_base.h"
#include <cstdint>

namespace oceanbase
{
namespace memtable
{

#define __HASH_HOLDER_ENABLE_MONITOR__

struct HashHolderGlocalMonitor {
  static constexpr int64_t BUCKET_SIZE = 64;
  static constexpr int64_t BUCKET_MASK = BUCKET_SIZE - 1;
  HashHolderGlocalMonitor() {
    for (int64_t idx = 0; idx < BUCKET_SIZE; ++idx) {
      new (&bucket_[idx]) MonitorBucket();
    }
  }
  struct MonitorBucket {
    MonitorBucket()
    :history_dynamic_alloc_node_cnt_(0),
    history_dynamic_free_node_cnt_(0),
    history_dynamic_alloc_list_cnt_(0),
    history_dynamic_free_list_cnt_(0),
    valid_bucket_cnt_(0),
    valid_list_cnt_(0),
    valid_node_cnt_(0) {}
    std::atomic<int64_t> history_dynamic_alloc_node_cnt_;
    std::atomic<int64_t> history_dynamic_free_node_cnt_;
    std::atomic<int64_t> history_dynamic_alloc_list_cnt_;
    std::atomic<int64_t> history_dynamic_free_list_cnt_;
    std::atomic<int64_t> valid_bucket_cnt_;
    std::atomic<int64_t> valid_list_cnt_;
    std::atomic<int64_t> valid_node_cnt_;
  };
  void add_history_dynamic_alloc_node_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].history_dynamic_alloc_node_cnt_.fetch_add(1, std::memory_order_relaxed);
  }
  void add_history_dynamic_free_node_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].history_dynamic_free_node_cnt_.fetch_add(1, std::memory_order_relaxed);
  }
  void add_history_dynamic_alloc_list_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].history_dynamic_alloc_list_cnt_.fetch_add(1, std::memory_order_relaxed);
  }
  void add_history_dynamic_free_list_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].history_dynamic_free_list_cnt_.fetch_add(1, std::memory_order_relaxed);
  }
  void inc_valid_bucket_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].valid_bucket_cnt_.fetch_add(1, std::memory_order_relaxed);
  }
  void dec_valid_bucket_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].valid_bucket_cnt_.fetch_add(-1, std::memory_order_relaxed);
  }
  void inc_valid_list_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].valid_list_cnt_.fetch_add(1, std::memory_order_relaxed);
  }
  void dec_valid_list_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].valid_list_cnt_.fetch_add(-1, std::memory_order_relaxed);
  }
  void inc_valid_node_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].valid_node_cnt_.fetch_add(1, std::memory_order_relaxed);
  }
  void dec_valid_node_cnt(uint64_t hash) {
    bucket_[hash & BUCKET_MASK].valid_node_cnt_.fetch_add(-1, std::memory_order_relaxed);
  }
  #define FOREACH_BUCKET_TO_SUM(element) \
  int64_t sum = 0; \
  for (int64_t idx = 0; idx < BUCKET_SIZE; ++idx) { \
    sum += bucket_[idx].element.load(std::memory_order_relaxed); \
  } \
  return sum
  int64_t get_history_dynamic_alloc_node_cnt() const { FOREACH_BUCKET_TO_SUM(history_dynamic_alloc_node_cnt_); }
  int64_t get_history_dynamic_free_node_cnt() const { FOREACH_BUCKET_TO_SUM(history_dynamic_free_node_cnt_); }
  int64_t get_history_dynamic_alloc_list_cnt() const { FOREACH_BUCKET_TO_SUM(history_dynamic_alloc_list_cnt_); }
  int64_t get_history_dynamic_free_list_cnt() const { FOREACH_BUCKET_TO_SUM(history_dynamic_free_list_cnt_); }
  int64_t get_valid_bucket_cnt() const { FOREACH_BUCKET_TO_SUM(valid_bucket_cnt_); }
  int64_t get_valid_list_cnt() const { FOREACH_BUCKET_TO_SUM(valid_list_cnt_); }
  int64_t get_valid_node_cnt() const { FOREACH_BUCKET_TO_SUM(valid_node_cnt_); }
  #undef FOREACH_BUCKET_TO_SUM
  MonitorBucket bucket_[BUCKET_SIZE];
};

}
}

#endif