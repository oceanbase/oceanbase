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

#ifndef OB_STORAGE_TABLE_ACCESS_CONTEXT_H
#define OB_STORAGE_TABLE_ACCESS_CONTEXT_H

#include "ob_table_access_param.h"
#include "storage/lob/ob_lob_locator.h"
#include "storage/tx/ob_defensive_check_mgr.h"
#include "share/scn.h"

namespace oceanbase
{
namespace compaction
{
struct ObCachedTransStateMgr;
}
namespace common
{
class ObIOCallback;
} // namespace common
namespace storage
{
class ObStoreRowIterPool;
class ObBlockRowStore;

struct ObRowStat
{
  int64_t base_row_count_;
  int64_t inc_row_count_;
  int64_t merge_row_count_;
  int64_t result_row_count_;
  int64_t filt_del_count_;

  ObRowStat() : base_row_count_(0), inc_row_count_(0), merge_row_count_(0), result_row_count_(0), filt_del_count_(0) {}

  void reset()
  {
    base_row_count_ = 0;
    inc_row_count_ = 0;
    merge_row_count_ = 0;
    result_row_count_ = 0;
    filt_del_count_ = 0;
  }
  TO_STRING_KV(K_(base_row_count), K_(inc_row_count), K_(merge_row_count), K_(result_row_count), K_(filt_del_count));
};

struct ObTableAccessContext
{
  ObTableAccessContext();
  virtual ~ObTableAccessContext();
  void reset();
  void reuse();
  inline bool is_valid() const {
    return is_inited_
      && NULL != store_ctx_
      && NULL != stmt_allocator_
      && NULL != allocator_; }
  inline bool enable_get_row_cache() const {
    return query_flag_.is_use_row_cache() && !use_fuse_row_cache_ && table_store_stat_.enable_get_row_cache() && !need_scn_ && !tablet_id_.is_ls_inner_tablet();
  }
  inline bool enable_put_row_cache() const {
    return query_flag_.is_use_row_cache() && !use_fuse_row_cache_ && table_store_stat_.enable_put_row_cache() && !need_scn_ && !tablet_id_.is_ls_inner_tablet();
  }
  inline bool enable_bf_cache() const {
    return query_flag_.is_use_bloomfilter_cache() && table_store_stat_.enable_bf_cache() && !need_scn_ && !tablet_id_.is_ls_inner_tablet();
  }
  inline bool enable_sstable_bf_cache() const {
    return query_flag_.is_use_bloomfilter_cache() && table_store_stat_.enable_sstable_bf_cache() && !need_scn_ && !tablet_id_.is_ls_inner_tablet();
  }
  inline bool is_multi_version_read(const int64_t snapshot_version) {
    return trans_version_range_.snapshot_version_ < snapshot_version;
  }
  inline bool enable_get_fuse_row_cache(const int64_t threshold) const {
    return query_flag_.is_use_fuse_row_cache() && table_store_stat_.enable_get_fuse_row_cache(threshold) && !need_scn_ && !tablet_id_.is_ls_inner_tablet();
  }
  inline bool enable_put_fuse_row_cache(const int64_t threshold) const {
    return query_flag_.is_use_fuse_row_cache() && table_store_stat_.enable_put_fuse_row_cache(threshold) && !need_scn_ && !tablet_id_.is_ls_inner_tablet();
  }
  inline bool is_limit_end() const {
    return (nullptr != limit_param_ && limit_param_->limit_ >= 0 && (out_cnt_ - limit_param_->offset_ >= limit_param_->limit_));
  }
  inline common::ObIAllocator *get_range_allocator() {
    return nullptr == range_allocator_ ? allocator_ : range_allocator_;
  }
  inline void reset_lob_locator_helper() {
    if (OB_NOT_NULL(lob_locator_helper_)) {
      lob_locator_helper_->~ObLobLocatorHelper();
      lob_locator_helper_ = nullptr;
      lob_allocator_.reset();
    }
  }
  // used for query
  int init(ObTableScanParam &scan_param,
           ObStoreCtx &ctx,
           const common::ObVersionRange &trans_version_range);
  // used for merge
  int init(const common::ObQueryFlag &query_flag,
           ObStoreCtx &ctx,
           common::ObIAllocator &allocator,
           common::ObIAllocator &stmt_allocator,
           const common::ObVersionRange &trans_version_range);
  // used for exist or simple scan
  int init(const common::ObQueryFlag &query_flag,
           ObStoreCtx &ctx,
           common::ObIAllocator &allocator,
           const common::ObVersionRange &trans_version_range);
  TO_STRING_KV(
    K_(is_inited),
    K_(timeout),
    K_(ls_id),
    K_(tablet_id),
    K_(query_flag),
    K_(sql_mode),
    KP_(store_ctx),
    KP_(limit_param),
    KP_(stmt_allocator),
    KP_(allocator),
    KP_(range_allocator),
    KP_(table_scan_stat),
    K_(out_cnt),
    K_(trans_version_range),
    K_(merge_scn),
    K_(lob_allocator),
    K_(lob_locator_helper),
    KP_(iter_pool),
    KP_(block_row_store),
    KP_(io_callback))
private:
  static const int64_t DEFAULT_COLUMN_SCALE_INFO_SIZE = 8;
  int build_lob_locator_helper(ObTableScanParam &scan_param,
                               const ObStoreCtx &ctx,
                               const common::ObVersionRange &trans_version_range);
  int build_lob_locator_helper(const ObStoreCtx &ctx,
                               const ObVersionRange &trans_version_range); // local scan
  // init need_fill_scale_ and search column which need fill scale
  int init_column_scale_info(ObTableScanParam &scan_param);

public:
  bool is_inited_;
  bool use_fuse_row_cache_; // temporary code
  bool need_scn_;
  int64_t timeout_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObQueryFlag query_flag_;
  ObSQLMode sql_mode_;
  ObStoreCtx *store_ctx_;
  common::ObLimitParam *limit_param_;
  // sql statement level allocator, available before sql execute finish
  common::ObIAllocator *stmt_allocator_;
  // storage scan/rescan interface level allocator, will be reclaimed in every scan/rescan call
  common::ObIAllocator *allocator_;
  // scan/rescan level alloctor in storage, will be reclaimed in every reuse/open call
  common::ObIAllocator *range_allocator_;
  lib::MemoryContext scan_mem_; // scan/rescan level memory entity, only for query
  common::ObTableScanStatistic *table_scan_stat_;
  ObTableStoreStat table_store_stat_;
  int64_t out_cnt_;
  common::ObVersionRange trans_version_range_;
  const common::ObSEArray<int64_t, 4, common::ModulePageAllocator> *range_array_pos_;
  share::SCN merge_scn_;
  common::ObArenaAllocator lob_allocator_;
  ObLobLocatorHelper *lob_locator_helper_;
  ObStoreRowIterPool *iter_pool_;
  ObBlockRowStore *block_row_store_;
  common::ObIOCallback *io_callback_;
  compaction::ObCachedTransStateMgr *trans_state_mgr_;
#ifdef ENABLE_DEBUG_LOG
  transaction::ObDefensiveCheckRecordExtend defensive_check_record_;
#endif
};

} // namespace storage
} // namespace oceanbase
#endif
