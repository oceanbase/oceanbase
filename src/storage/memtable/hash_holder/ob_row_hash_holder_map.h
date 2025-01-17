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

#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_MAP_H_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_MAP_H_

#include "ob_row_hash_holder_cache.ipp"
#include "storage/memtable/hash_holder/ob_row_hash_holder_allocator.h"
#include "ob_row_hash_holder_monitor.h"

namespace oceanbase
{
namespace memtable
{

struct RowHolderBucketHead {// RAII + thread safe
  RowHolderBucketHead() : list_(nullptr), map_(nullptr), lock_() {}
  RowHolderBucketHead(RowHolderMapper *map) : list_(nullptr), map_(map), lock_() {}
  ~RowHolderBucketHead();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t size() const;// O(N), just for unittest
  int insert(const uint64_t hash,
             const transaction::ObTransID tx_id,
             const transaction::ObTxSEQ seq,
             const share::SCN scn,
             HashHolderGlocalMonitor &monitor);
  int erase(const uint64_t hash,
            const transaction::ObTransID tx_id,
            const transaction::ObTxSEQ seq,
            HashHolderGlocalMonitor &monitor);
  int get(const uint64_t hash,
          RowHolderInfo &holder_info,
          HashHolderGlocalMonitor &monitor) const;
  int find_or_create_hash_list_(const uint64_t hash,
                                const bool create_if_not_exist,
                                RowHolderList *&list,
                                RowHolderList **&link_found_list_ptr,
                                HashHolderGlocalMonitor &monitor);
  void destroy_hash_list_if_not_valid_(const uint64_t hash,
                                       RowHolderList **link_deleted_list_ptr,
                                       RowHolderList *deleted_list,
                                       HashHolderGlocalMonitor &monitor);
  bool is_empty() const { return !list_; }
  void clear();
  RowHolderList *list_;
  RowHolderMapper *map_;
  mutable ObByteLock lock_;
} CACHE_ALIGNED;

struct RowHolderMapper {// RAII + thread safe
  RowHolderMapper(const RowHolderMapper &) = delete;
  RowHolderMapper &operator=(const RowHolderMapper &) = delete;
  RowHolderMapper()
  : factory_(monitor_),
  bucket_head_(nullptr),
  bucket_cnt_(0),
  monitor_() {}
  ~RowHolderMapper();
  int init(bool for_unit_test);
  void clear();
  int64_t count() const;
  void periodic_tasks();
  void print_summary_info_to_buffer_(char *buffer, int64_t &pos, const int64_t buffer_len) const;
  // 1. leader mvcc_write
  // 2. follower mvcc_replay
  // there maybe hash confict, if meet conflict, replace hash holder
  void insert_or_replace_hash_holder(const uint64_t hash,
                                     const transaction::ObTransID tx_id,
                                     const transaction::ObTxSEQ seq,
                                     const share::SCN scn);
  // 1. trans node set_delay_clean_out
  // 2. trans node commit
  // 3. trans node rollback
  // 4. trans node abort
  // 5. trans node elr
  // there maybe hash conflict, if meet conflict, can not do erase action
  void erase_hash_holder_record(const uint64_t hash,
                                const transaction::ObTransID tx_id,
                                const transaction::ObTxSEQ seq);
  // read holder info to detect deadlock
  int get_hash_holder(uint64_t hash, RowHolderInfo &holder_info) const;
  HashHolderFactory factory_;
  RowHolderBucketHead *bucket_head_;
  int64_t bucket_cnt_;
  HashHolderGlocalMonitor monitor_;
  // delegate
  DELEGATE(factory_, create);
  DELEGATE(factory_, destroy);
};

}
}

#endif