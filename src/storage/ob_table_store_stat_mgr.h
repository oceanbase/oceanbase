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

#ifndef OB_TABLE_STORE_STAT_MGR_H_
#define OB_TABLE_STORE_STAT_MGR_H_
#include <stdint.h>
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/task/ob_timer.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace storage {
struct ObMergeIterStat {
public:
  ObMergeIterStat()
  {
    reset();
  };
  ~ObMergeIterStat() = default;
  OB_INLINE void reset()
  {
    MEMSET(this, 0, sizeof(ObMergeIterStat));
  }
  bool is_valid() const;
  int add(const ObMergeIterStat& other);
  ObMergeIterStat& operator=(const ObMergeIterStat& other);
  TO_STRING_KV(K_(call_cnt), K_(output_row_cnt));

  int64_t call_cnt_;
  int64_t output_row_cnt_;
};

struct ObBlockAccessStat {
public:
  ObBlockAccessStat()
  {
    reset();
  };
  ~ObBlockAccessStat() = default;
  OB_INLINE void reset()
  {
    MEMSET(this, 0, sizeof(ObBlockAccessStat));
  }
  bool is_valid() const;
  int add(const ObBlockAccessStat& other);
  ObBlockAccessStat& operator=(const ObBlockAccessStat& other);
  TO_STRING_KV(K_(effect_read_cnt), K_(empty_read_cnt));

  int64_t effect_read_cnt_;
  int64_t empty_read_cnt_;
};

struct ObTableStoreStat {
public:
  ObTableStoreStat();
  ~ObTableStoreStat() = default;

  void reset();
  void reuse();
  bool is_valid() const;
  int add(const ObTableStoreStat& other);
  ObTableStoreStat& operator=(const ObTableStoreStat& other);
  TO_STRING_KV(K_(pkey), K_(row_cache_hit_cnt), K_(row_cache_miss_cnt), K_(row_cache_put_cnt), K_(bf_filter_cnt),
      K_(bf_empty_read_cnt), K_(bf_access_cnt), K_(block_cache_hit_cnt), K_(block_cache_miss_cnt), K_(access_row_cnt),
      K_(output_row_cnt), K_(fuse_row_cache_hit_cnt), K_(fuse_row_cache_miss_cnt), K_(fuse_row_cache_put_cnt),
      K_(single_get_stat), K_(multi_get_stat), K_(index_back_stat), K_(single_scan_stat), K_(multi_scan_stat),
      K_(exist_row), K_(get_row), K_(scan_row));

  common::ObPartitionKey pkey_;
  int64_t row_cache_hit_cnt_;
  int64_t row_cache_miss_cnt_;
  int64_t row_cache_put_cnt_;
  int64_t bf_filter_cnt_;
  int64_t bf_empty_read_cnt_;
  int64_t bf_access_cnt_;
  int64_t block_cache_hit_cnt_;
  int64_t block_cache_miss_cnt_;
  int64_t access_row_cnt_;
  int64_t output_row_cnt_;
  int64_t fuse_row_cache_hit_cnt_;
  int64_t fuse_row_cache_miss_cnt_;
  int64_t fuse_row_cache_put_cnt_;
  ObMergeIterStat single_get_stat_;
  ObMergeIterStat multi_get_stat_;
  ObMergeIterStat index_back_stat_;  // index back only works in multi_get mode
  ObMergeIterStat single_scan_stat_;
  ObMergeIterStat multi_scan_stat_;
  ObBlockAccessStat exist_row_;
  ObBlockAccessStat get_row_;
  ObBlockAccessStat scan_row_;
};

struct ObTableStoreStatKey {
public:
  ObTableStoreStatKey() : table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_ID)
  {}
  ObTableStoreStatKey(const uint64_t table_id, const int64_t partition_id)
      : table_id_(table_id), partition_id_(partition_id)
  {}
  ~ObTableStoreStatKey()
  {}
  OB_INLINE uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&table_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(&partition_id_, sizeof(int64_t), hash_ret);
    return hash_ret;
  }
  OB_INLINE bool operator==(const ObTableStoreStatKey& other) const
  {
    return (table_id_ == other.table_id_) && (partition_id_ == other.partition_id_);
  }
  OB_INLINE bool operator!=(const ObTableStoreStatKey& other) const
  {
    return (*this == other);
  }
  TO_STRING_KV(K_(table_id), K_(partition_id));
  uint64_t table_id_;
  int64_t partition_id_;
};

struct ObTableStoreStatNode {
public:
  ObTableStoreStatNode() : pre_(NULL), next_(NULL), stat_(NULL)
  {}
  ~ObTableStoreStatNode()
  {
    reset();
  }
  OB_INLINE void reset()
  {
    pre_ = next_ = NULL;
    stat_ = NULL;
  }
  ObTableStoreStatNode* pre_;
  ObTableStoreStatNode* next_;
  ObTableStoreStat* stat_;
};

class ObTableStoreStatIterator {
public:
  ObTableStoreStatIterator();
  virtual ~ObTableStoreStatIterator();
  int open();
  int get_next_stat(ObTableStoreStat& stat);
  void reset();

private:
  int64_t cur_idx_;
  bool is_opened_;
};

class ObTableStoreStatMgr {
public:
  int init(const int64_t limit_cnt = DEFAULT_MAX_CNT);
  void destroy();
  static ObTableStoreStatMgr& get_instance();
  int report_stat(const ObTableStoreStat& stat);

private:
  ObTableStoreStatMgr();
  virtual ~ObTableStoreStatMgr();
  void move_node_to_head(ObTableStoreStatNode* node);
  int get_table_store_stat(const int64_t idx, ObTableStoreStat& stat);
  void run_report_task();
  int add_stat(const ObTableStoreStat& stat);

  friend class ObTableStoreStatIterator;
  typedef common::hash::ObHashMap<ObTableStoreStatKey, ObTableStoreStatNode*, common::hash::NoPthreadDefendMode>
      TableStoreMap;
  static const int64_t DEFAULT_MAX_CNT =
      40000;  // 40000 * (sizeof(key)(16) + sizeof(node*)(8) + sizeof(node)(24) + sizeof(stat)(96)) = 6.25MB
  static const int64_t MAX_PENDDING_CNT = 100000;              // 100000 * sizeof(stat)(96) = 9.2MB
  static const int64_t REPORT_TASK_INTERVAL_US = 1000 * 1000;  // 1 seconds

  class ReportTask : public common::ObTimerTask {
  public:
    ReportTask() : stat_mgr_(NULL)
    {}
    virtual ~ReportTask()
    {
      destroy();
    }
    int init(ObTableStoreStatMgr* stat_mgr);
    void destroy()
    {
      stat_mgr_ = NULL;
    }
    virtual void runTimerTask();

  private:
    ObTableStoreStatMgr* stat_mgr_;
    DISALLOW_COPY_AND_ASSIGN(ReportTask);
  };

  bool is_inited_;
  common::SpinRWLock lock_;
  TableStoreMap quick_map_;
  int64_t cur_cnt_;
  int64_t limit_cnt_;
  ObTableStoreStatNode* lru_head_;
  ObTableStoreStatNode* lru_tail_;
  ObTableStoreStat stat_array_[DEFAULT_MAX_CNT];
  ObTableStoreStatNode node_pool_[DEFAULT_MAX_CNT];

  // stat buffer area
  ObTableStoreStat stat_queue_[MAX_PENDDING_CNT];
  uint64_t report_cursor_ CACHE_ALIGNED;
  uint64_t pending_cursor_ CACHE_ALIGNED;
  ReportTask report_task_;
};

}  // namespace storage
}  // namespace oceanbase
#endif /* OB_TABLE_STORE_STAT_MGR_H_ */
