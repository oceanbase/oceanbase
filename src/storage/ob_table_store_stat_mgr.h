/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_TABLE_STORE_STAT_MGR_H_
#define OB_TABLE_STORE_STAT_MGR_H_
#include <stdint.h>
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{
struct ObMergeIterStat
{
public:
  ObMergeIterStat() { reset(); };
  ~ObMergeIterStat() = default;
  OB_INLINE void reset() { MEMSET(this, 0, sizeof(ObMergeIterStat)); }
  bool is_valid() const;
  int add(const ObMergeIterStat& other);
  ObMergeIterStat & operator=(const ObMergeIterStat &other);
  TO_STRING_KV(K_(call_cnt), K_(output_row_cnt));

  int64_t call_cnt_;
  int64_t output_row_cnt_;
};

struct ObBlockAccessStat
{
public:
  ObBlockAccessStat() { reset(); };
  ~ObBlockAccessStat() = default;
  OB_INLINE void reset() { MEMSET(this, 0, sizeof(ObBlockAccessStat)); }
  bool is_valid() const;
  int add(const ObBlockAccessStat& other);
  ObBlockAccessStat & operator=(const ObBlockAccessStat &other);
  TO_STRING_KV(K_(effect_read_cnt), K_(empty_read_cnt));

  int64_t effect_read_cnt_;
  int64_t empty_read_cnt_;
};

struct ObTableStoreStat
{
public:
  ObTableStoreStat();
  ~ObTableStoreStat() = default;

  void reset();
  void reuse();
  bool is_valid() const;
  int add(const ObTableStoreStat& other);
  ObTableStoreStat &operator=(const ObTableStoreStat& other);
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(table_id),
               K_(row_cache_hit_cnt), K_(row_cache_miss_cnt), K_(row_cache_put_cnt),
               K_(bf_filter_cnt), K_(bf_empty_read_cnt), K_(bf_access_cnt),
               K_(block_cache_hit_cnt), K_(block_cache_miss_cnt),
               K_(access_row_cnt), K_(output_row_cnt), K_(fuse_row_cache_hit_cnt),
               K_(fuse_row_cache_miss_cnt), K_(fuse_row_cache_put_cnt),
               K_(macro_access_cnt), K_(micro_access_cnt), K_(pushdown_micro_access_cnt),
               K_(pushdown_row_access_cnt), K_(pushdown_row_select_cnt),
               K_(single_get_stat), K_(multi_get_stat), K_(index_back_stat),
               K_(single_scan_stat), K_(multi_scan_stat),
               K_(exist_row), K_(get_row), K_(scan_row),
               K_(sstable_bf_filter_cnt), K_(sstable_bf_empty_read_cnt),
               K_(sstable_bf_access_cnt), K_(rowkey_prefix));

  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTableID table_id_;
  int64_t row_cache_hit_cnt_;
  int64_t row_cache_miss_cnt_;
  int64_t row_cache_put_cnt_;
  int64_t bf_filter_cnt_;
  int64_t bf_empty_read_cnt_;
  int64_t bf_access_cnt_;
  int64_t block_cache_hit_cnt_;
  int64_t block_cache_miss_cnt_;
  int64_t index_block_cache_hit_cnt_;
  int64_t index_block_cache_miss_cnt_;
  int64_t access_row_cnt_;
  int64_t output_row_cnt_;
  int64_t fuse_row_cache_hit_cnt_;
  int64_t fuse_row_cache_miss_cnt_;
  int64_t fuse_row_cache_put_cnt_;
  int64_t macro_access_cnt_;
  int64_t micro_access_cnt_;
  int64_t pushdown_micro_access_cnt_;
  int64_t pushdown_row_access_cnt_;
  int64_t pushdown_row_select_cnt_;
  ObMergeIterStat single_get_stat_;
  ObMergeIterStat multi_get_stat_;
  ObMergeIterStat index_back_stat_; // index back only works in multi_get mode
  ObMergeIterStat single_scan_stat_;
  ObMergeIterStat multi_scan_stat_;
  ObBlockAccessStat exist_row_;
  ObBlockAccessStat get_row_;
  ObBlockAccessStat scan_row_;
  int64_t sstable_bf_filter_cnt_;
  int64_t sstable_bf_empty_read_cnt_;
  int64_t sstable_bf_access_cnt_;
  int64_t rowkey_prefix_;
};

class ObTableStoreStatIterator
{
public:
  ObTableStoreStatIterator();
  virtual ~ObTableStoreStatIterator();
  int open();
  int get_next_stat(ObTableStoreStat &stat);
  void reset();
private:
  int64_t cur_idx_;
  bool is_opened_;
};

} //namespace storage
} //namespace oceanbase
#endif /* OB_TABLE_STORE_STAT_MGR_H_ */
