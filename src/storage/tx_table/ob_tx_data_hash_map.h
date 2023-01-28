
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

#ifndef OCEANBASE_STORAGE_OB_TX_DATA_HASHMAP_
#define OCEANBASE_STORAGE_OB_TX_DATA_HASHMAP_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_slice_alloc.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase {

namespace transaction {
class ObTransID;
}

namespace storage {
class ObTxData;
class ObTxDataGuard;

class ObTxDataHashMap {
private:
  static const int64_t MAX_CONCURRENCY = 32;
  static const int64_t MAX_CONCURRENCY_MASK = MAX_CONCURRENCY - 1;

public:
  ObTxDataHashMap(const uint64_t buckets_cnt)
      : BUCKETS_CNT(buckets_cnt),
        BUCKETS_MOD_MASK(buckets_cnt - 1),
        buckets_(nullptr),
        total_cnt_(0) {}
  ~ObTxDataHashMap()
  {
    destroy();
  }

  int init();
  virtual void destroy();

  int insert(const transaction::ObTransID &key, ObTxData *value);
  int get(const transaction::ObTransID &key, ObTxDataGuard &guard);

  OB_INLINE int64_t get_pos(const transaction::ObTransID key)
  {
    return key.hash() & BUCKETS_MOD_MASK;
  }

  OB_INLINE int64_t get_buckets_cnt()
  {
    return BUCKETS_CNT;
  }

  OB_INLINE int64_t count() const
  {
    return ATOMIC_LOAD(&total_cnt_);
  }

private:
  struct ObTxDataHashHeader {
    ObTxData *next_;
    ObTxData *hot_cache_val_;

    ObTxDataHashHeader() : next_(nullptr), hot_cache_val_(nullptr) {}
    ~ObTxDataHashHeader()
    {
      destroy();
    }
    void reset()
    {
      next_ = nullptr;
      hot_cache_val_ = nullptr;
    }
    void destroy() { reset(); }
  };

private:
  const int64_t BUCKETS_CNT;
  const int64_t BUCKETS_MOD_MASK;
  ObTxDataHashHeader *buckets_;
  int64_t total_cnt_;

public:
  class Iterator {
  public:
    Iterator(ObTxDataHashMap &tx_data_map)
        : bucket_idx_(-1), val_(nullptr), tx_data_map_(tx_data_map)
    {}

    int get_next(ObTxDataGuard &next_val);

  public:
    int64_t bucket_idx_;
    ObTxData *val_;
    ObTxDataHashMap &tx_data_map_;
  };
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_TX_DATA_HASHMAP_
