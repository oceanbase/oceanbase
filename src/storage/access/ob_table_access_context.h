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
#include "ob_sample_filter.h"
#include "storage/lob/ob_lob_locator.h"
#include "storage/tx/ob_defensive_check_mgr.h"
#include "share/scn.h"
#include "storage/access/ob_micro_block_handle_mgr.h"
#include "storage/column_store/ob_i_cg_iterator.h"

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
template<typename T>
class ObStoreRowIterPool;
class ObBlockRowStore;
class ObCGIterParamPool;
struct ObTableScanRange;

#define REALTIME_MONITOR_ADD_IO_READ_BYTES(CTX, SIZE) \
  if (OB_NOT_NULL(CTX)) CTX->add_io_read_bytes(SIZE)  \

#define REALTIME_MONITOR_ADD_SSSTORE_READ_BYTES(CTX, SIZE) \
  if (OB_NOT_NULL(CTX)) CTX->add_ssstore_read_bytes(SIZE)

#define REALTIME_MONITOR_INC_READ_ROW_CNT(ITER, CTX) \
  if (OB_LIKELY(nullptr != ITER && nullptr != CTX))  \
    ITER->is_sstable_iter() ? CTX->add_ssstore_read_row_cnt() : CTX->add_memstore_read_row_cnt();

#define REALTIME_MONITOR_ADD_READ_ROW_CNT(CTX, COUNT) \
  if (OB_NOT_NULL(CTX)) CTX->add_ssstore_read_row_cnt(COUNT);

struct ObTableScanStoreStat
{
  ObTableScanStoreStat() { reset(); }
  ~ObTableScanStoreStat() = default;
  OB_INLINE void reset()
  {
    MEMSET(this, 0, sizeof(ObTableScanStoreStat));
  }
  // for rescan query, keep row cache and bf releated stats unchanged
  OB_INLINE void reuse()
  {
    block_cache_hit_cnt_ = 0;
    block_cache_miss_cnt_ = 0;
    micro_access_cnt_ = 0;
    pushdown_micro_access_cnt_ = 0;
    empty_read_cnt_ = 0;
    rowkey_prefix_ = 0;
    logical_read_cnt_ = 0;
    physical_read_cnt_ = 0;
  }
public:
  OB_INLINE bool enable_get_row_cache() const
  {
    return row_cache_miss_cnt_ < common::MAX_MULTI_GET_CACHE_AWARE_ROW_NUM
           || row_cache_hit_cnt_ > row_cache_miss_cnt_ / 2;
  }
  OB_INLINE bool enable_put_row_cache() const
  {
    return row_cache_put_cnt_ < common::MAX_MULTI_GET_CACHE_AWARE_ROW_NUM;
  }
  OB_INLINE bool enable_put_fuse_row_cache(const int64_t threshold) const
  {
    return fuse_row_cache_put_cnt_ < threshold;
  }
  OB_INLINE bool enable_get_fuse_row_cache(const int64_t threshold) const
  {
    return fuse_row_cache_miss_cnt_ < threshold
           || fuse_row_cache_hit_cnt_ > fuse_row_cache_miss_cnt_ / 4;
  }
  OB_INLINE bool enable_bf_cache() const
  {
    return (bf_access_cnt_ < common::MAX_MULTI_GET_CACHE_AWARE_ROW_NUM
           || bf_filter_cnt_ > (bf_access_cnt_ / 8));
  }
  TO_STRING_KV(K_(row_cache_hit_cnt), K_(row_cache_miss_cnt), K_(row_cache_put_cnt),
               K_(bf_filter_cnt), K_(bf_access_cnt),
               K_(block_cache_hit_cnt), K_(block_cache_miss_cnt),
               K_(fuse_row_cache_hit_cnt), K_(fuse_row_cache_miss_cnt), K_(fuse_row_cache_put_cnt),
               K_(micro_access_cnt), K_(pushdown_micro_access_cnt),
               K_(empty_read_cnt), K_(rowkey_prefix),
               K_(logical_read_cnt), K_(physical_read_cnt));
  int64_t row_cache_hit_cnt_;
  int64_t row_cache_miss_cnt_;
  int64_t row_cache_put_cnt_;
  int64_t bf_filter_cnt_;
  int64_t bf_access_cnt_;
  int64_t block_cache_hit_cnt_;
  int64_t block_cache_miss_cnt_;
  int64_t fuse_row_cache_hit_cnt_;
  int64_t fuse_row_cache_miss_cnt_;
  int64_t fuse_row_cache_put_cnt_;
  int64_t micro_access_cnt_;
  int64_t pushdown_micro_access_cnt_;
  int64_t empty_read_cnt_;
  int64_t rowkey_prefix_;
  int64_t logical_read_cnt_;
  int64_t physical_read_cnt_;
};

struct ObTableAccessContext
{
  ObTableAccessContext();
  virtual ~ObTableAccessContext();
  void reset();
  void reuse();
  int rescan_reuse(ObTableScanParam &scan_param);
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
  inline bool is_multi_version_read(const int64_t snapshot_version) {
    return trans_version_range_.snapshot_version_ < snapshot_version;
  }
  inline bool enable_get_fuse_row_cache(const int64_t threshold) const {
    return query_flag_.is_use_fuse_row_cache() && table_store_stat_.enable_get_fuse_row_cache(threshold) && !need_scn_ && !tablet_id_.is_ls_inner_tablet();
  }
  inline bool enable_put_fuse_row_cache(const int64_t threshold, const bool is_mview_table_scan) const {
    return query_flag_.is_use_fuse_row_cache() && table_store_stat_.enable_put_fuse_row_cache(threshold) && (!need_scn_ || is_mview_table_scan) && !tablet_id_.is_ls_inner_tablet();
  }
  inline bool is_limit_end() const {
    return (nullptr != limit_param_ && limit_param_->limit_ >= 0 && (out_cnt_ - limit_param_->offset_ >= limit_param_->limit_));
  }
  inline sql::ObPushdownFilterExecutor *get_sample_executor() {
    return nullptr == sample_filter_ ? nullptr : sample_filter_->get_sample_executor();
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
  inline void reuse_lob_locator_helper() {
    if (OB_NOT_NULL(lob_locator_helper_)) {
      lob_locator_helper_->reuse();
    }
  }
  inline void reset_cached_iter_node()
  {
    cached_iter_node_ = nullptr;
  }
  // used for query
  int init(ObTableScanParam &scan_param,
           ObStoreCtx &ctx,
           const common::ObVersionRange &trans_version_range,
           CachedIteratorNode *cached_iter_node);
  // used for merge and exist
  int init(const common::ObQueryFlag &query_flag,
           ObStoreCtx &ctx,
           common::ObIAllocator &allocator,
           common::ObIAllocator &stmt_allocator,
           const common::ObVersionRange &trans_version_range,
           const bool for_exist = false);
  // used for exist or simple scan
  int init(const common::ObQueryFlag &query_flag,
           ObStoreCtx &ctx,
           common::ObIAllocator &allocator,
           const common::ObVersionRange &trans_version_range,
           CachedIteratorNode *cached_iter_node = nullptr);
  // used for mview table scan
  int init_for_mview(common::ObIAllocator *allocator,
                     const ObTableAccessContext &access_ctx,
                     ObStoreCtx &store_ctx);
  OB_INLINE bool is_mview_query() const
  {
    return nullptr != mview_scan_info_;
  }
  OB_INLINE StorageScanType get_scan_type() const
  {
    return is_mview_query() ?  mview_scan_info_->scan_type_ : StorageScanType::NORMAL;
  }
  int alloc_iter_pool(const bool use_column_store);
  void inc_micro_access_cnt();
  int init_scan_allocator(ObTableScanParam &scan_param);
  int init_mview_scan_info(const int64_t multi_version_start, const sql::ObExprPtrIArray *op_filters, sql::ObEvalCtx &eval_ctx);
  // update realtime monitor info
  OB_INLINE void add_io_read_bytes(const int64_t bytes)
  {
    if (OB_LIKELY(nullptr != table_scan_stat_ && nullptr != table_scan_stat_->tsc_monitor_info_)) {
      *table_scan_stat_->tsc_monitor_info_->io_read_bytes_ += bytes;
    }
  }
  OB_INLINE void add_ssstore_read_bytes(const int64_t bytes)
  {
    if (OB_LIKELY(nullptr != table_scan_stat_ && nullptr != table_scan_stat_->tsc_monitor_info_)) {
      *table_scan_stat_->tsc_monitor_info_->ssstore_read_bytes_ += bytes;
    }
  }
  OB_INLINE void add_ssstore_read_row_cnt(const int64_t count = 1)
  {
    if (OB_LIKELY(nullptr != table_scan_stat_ && nullptr != table_scan_stat_->tsc_monitor_info_)) {
      *table_scan_stat_->tsc_monitor_info_->ssstore_read_row_cnt_ += count;
    }
  }
  OB_INLINE void add_memstore_read_row_cnt(const int64_t count = 1)
  {
    if (OB_LIKELY(nullptr != table_scan_stat_ && nullptr != table_scan_stat_->tsc_monitor_info_)) {
      *table_scan_stat_->tsc_monitor_info_->memstore_read_row_cnt_ += count;
    }
  }
  TO_STRING_KV(
    K_(is_inited),
    K_(use_fuse_row_cache),
    K_(need_scn),
    K_(need_release_mview_scan_info),
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
    KP_(cached_iter_node),
    KP_(stmt_iter_pool),
    KP_(cg_iter_pool),
    KP_(cg_param_pool),
    KP_(block_row_store),
    KP_(sample_filter),
    KPC_(mview_scan_info));
private:
  static const int64_t DEFAULT_COLUMN_SCALE_INFO_SIZE = 8;
  static const int64_t USE_BLOCK_CACHE_LIMIT = 128L << 10;  // 128K
  int build_lob_locator_helper(ObTableScanParam &scan_param,
                               const ObStoreCtx &ctx,
                               const common::ObVersionRange &trans_version_range);
  int build_lob_locator_helper(const ObStoreCtx &ctx,
                               const ObVersionRange &trans_version_range); // local scan
  // init need_fill_scale_ and search column which need fill scale
  int init_column_scale_info(ObTableScanParam &scan_param);
public:
  OB_INLINE common::ObIAllocator *get_long_life_allocator()
  {
    return nullptr == cached_iter_node_ ? stmt_allocator_ : cached_iter_node_->get_iter_allocator();
  }
  OB_INLINE ObStoreRowIterPool<ObStoreRowIterator> *get_stmt_iter_pool()
  {
    return nullptr == cached_iter_node_ ? stmt_iter_pool_ : cached_iter_node_->get_stmt_iter_pool();
  }
  bool is_inited_;
  bool use_fuse_row_cache_; // temporary code
  bool need_scn_;
  bool need_release_mview_scan_info_;
  int64_t timeout_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObQueryFlag query_flag_;
  ObSQLMode sql_mode_;
  ObMicroBlockHandleMgr micro_block_handle_mgr_;
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
  ObTableScanStoreStat table_store_stat_;
  int64_t out_cnt_;
  common::ObVersionRange trans_version_range_;
  const common::ObSEArray<int64_t, 4, common::ModulePageAllocator> *range_array_pos_;
  share::SCN merge_scn_;
  common::ObArenaAllocator lob_allocator_;
  ObLobLocatorHelper *lob_locator_helper_;
  CachedIteratorNode *cached_iter_node_;
  ObStoreRowIterPool<ObStoreRowIterator> *stmt_iter_pool_;
  ObStoreRowIterPool<ObICGIterator> *cg_iter_pool_;
  ObCGIterParamPool *cg_param_pool_;
  ObBlockRowStore *block_row_store_;
  ObRowSampleFilter *sample_filter_;
  compaction::ObCachedTransStateMgr *trans_state_mgr_;
  ObMviewScanInfo *mview_scan_info_;
};

} // namespace storage
} // namespace oceanbase
#endif
