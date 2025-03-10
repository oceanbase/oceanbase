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

#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "storage/memtable/mvcc/ob_tx_callback_hash_holder_helper.h"
#include "deps/oblib/src/lib/hash/ob_linear_hash_map.h"
#include "storage/memtable/ob_mt_hash.h"
#include "ob_row_holder_info.h"
#include <cstdint>

namespace oceanbase
{
namespace memtable
{
struct RowHolderMapper {// RAII + thread safe
  struct KeyWrapper {
  public:
    KeyWrapper() : v_(0) {}
    explicit KeyWrapper(const uint64_t v) : v_(v) {}
    ~KeyWrapper() = default;
    void reset() { v_ = 0; }
    uint64_t hash() const { return v_; }
    int hash(uint64_t &hash_val) const { hash_val = v_; return OB_SUCCESS; }
    int compare(const KeyWrapper &other) const;
    bool operator==(const KeyWrapper &other) const { return 0 == compare(other); }
    bool operator!=(const KeyWrapper &other) const { return !operator==(other); }
    TO_STRING_KV(K_(v));
  private:
    uint64_t v_;
  };
  RowHolderMapper(const RowHolderMapper &) = delete;
  RowHolderMapper &operator=(const RowHolderMapper &) = delete;
  RowHolderMapper()
  : holder_map_(),
  total_callback_cnt_(0) {}
  ~RowHolderMapper() = default;
  int init();
  void clear();
  int64_t list_count() const;
  int64_t node_count() const;
  void periodic_tasks();
  // 1. leader mvcc_write
  // 2. follower mvcc_replay
  // there maybe hash confict, if meet conflict, replace hash holder
  void insert_hash_holder(const uint64_t hash,
                          ObTxCallbackHashHolderLinker &linker,
                          bool reverse_insert);
  // 1. trans node set_delay_clean_out
  // 2. trans node commit
  // 3. trans node rollback
  // 4. trans node abort
  // 5. trans node elr
  // there maybe hash conflict, if meet conflict, can not do erase action
  void erase_hash_holder_record(const uint64_t hash,
                                ObTxCallbackHashHolderLinker &linker,
                                bool reverse_find);
  // read holder info to detect deadlock
  int get_hash_holder(uint64_t hash, RowHolderInfo &holder_info);
  common::ObLinearHashMap<KeyWrapper, ObTxCallbackHashHolderList> holder_map_;
  int64_t total_callback_cnt_;
};

}
}

#endif