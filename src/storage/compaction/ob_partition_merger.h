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

#ifndef OB_COMPACTION_PARTITION_MERGER_H_
#define OB_COMPACTION_PARTITION_MERGER_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_raw_se_array.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_bloom_filter_data_writer.h"
#include "storage/blocksstable/ob_bloom_filter_data_reader.h"
#include "ob_partition_merge_iter.h"
#include "ob_partition_merge_fuser.h"
#include "ob_partition_merge_progress.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/compaction/ob_sstable_merge_history.h"
#include "storage/blocksstable/ob_sstable.h"
#include "ob_tablet_merge_ctx.h"
#include "lib/container/ob_loser_tree.h"
#include "storage/compaction/ob_partition_rows_merger.h"
#include "storage/compaction/ob_compaction_trans_cache.h"
#include "ob_tablet_merge_task.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/compaction/ob_tablet_merge_info.h"
#include "storage/compaction/ob_progressive_merge_helper.h"
#include "storage/compaction/ob_uncommit_tx_info.h"
#include "storage/column_store/ob_co_merge_ctx.h"

namespace oceanbase
{
using namespace blocksstable;

namespace compaction
{
struct ObMergeParameter;
class ObPartitionMergeHelper;
class ObPartitionMinorMergeHelper;
class ObTabletMergeInfo;

struct ObMacroBlockOp {
  enum BlockOp: uint8_t {
    OP_NONE = 0,
    OP_REORG = 1,
    OP_REWRITE = 2
  };

  ObMacroBlockOp() = default;
  ~ObMacroBlockOp() = default;
  OB_INLINE void reset() { block_op_ = OP_NONE; }
  OB_INLINE bool is_none() const { return block_op_ == OP_NONE; }
  OB_INLINE bool is_rewrite() const { return block_op_ == OP_REWRITE; }
  OB_INLINE bool is_reorg() const { return block_op_ == OP_REORG; }
  OB_INLINE bool is_open() const { return is_reorg() || is_rewrite(); }
  OB_INLINE void set_rewrite() { block_op_ = OP_REWRITE; }
  OB_INLINE void set_reorg() { block_op_ = OP_REORG; }
  OB_INLINE bool is_valid() const { return block_op_ <= OP_REWRITE && block_op_ >= OP_NONE; }
  const char* get_block_op_str() const;

  TO_STRING_KV("op_type", get_block_op_str());

  BlockOp block_op_;
private:
  const static char * block_op_str_[];
};

class ObDataDescHelper final {
public:
  static int build(
      const ObMergeParameter &merge_param,
      ObTabletMergeInfo &input_merge_info,
      blocksstable::ObDataStoreDesc &data_store_desc);
};

struct ObMergerBasic
{
public:
  ObMergerBasic(compaction::ObLocalArena &allocator, const ObStaticMergeParam &static_param)
    : merger_arena_(allocator),
      merge_ctx_(nullptr),
      task_idx_(0),
      merge_param_(static_param)
  {}
  virtual ~ObMergerBasic() { reset(); }
  virtual void reset();
  virtual int basic_prepare(ObBasicTabletMergeCtx &ctx, const int64_t idx);
  int get_all_majors(ObIArray<ObITable*> &tables);
  // for co merge
  template <typename T>
  int alloc_row_writer(
    const int64_t cg_idx,
    const blocksstable::ObDatumRow &default_row,
    ObIArray<ObITable*> &tables,
    T *&writer);
  static const int64_t CACHED_TRANS_STATE_MAX_CNT = 10 * 1024l;
  VIRTUAL_TO_STRING_KV(K_(task_idx), K_(merge_param));
public:
  compaction::ObLocalArena &merger_arena_;
  ObBasicTabletMergeCtx *merge_ctx_;
  int64_t task_idx_;
  ObMergeParameter merge_param_;
};

struct ObMerger : public ObMergerBasic
{
public:
  ObMerger(compaction::ObLocalArena &allocator, const ObStaticMergeParam &static_param)
    : ObMergerBasic(allocator, static_param),
      partition_fuser_(nullptr),
      merge_helper_(nullptr),
      filter_statistics_()
  {}
  virtual ~ObMerger() { reset(); }
  virtual void reset();
  virtual int basic_prepare(ObBasicTabletMergeCtx &ctx, const int64_t idx);
protected:
  int try_filter_row(const blocksstable::ObDatumRow &row, ObICompactionFilter::ObFilterRet &filter_ret);
public:
  ObIPartitionMergeFuser *partition_fuser_;
  ObPartitionMergeHelper *merge_helper_;
  ObICompactionFilter::ObFilterStatistics filter_statistics_;
};

class ObRowStoreMerger : public ObMerger
{
public:
  ObRowStoreMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  virtual ~ObRowStoreMerger() { reset(); }
  void virtual reset();
  virtual int merge_partition(
      ObBasicTabletMergeCtx &ctx,
      const int64_t idx) = 0;
  void force_flat_format() { force_flat_format_ = true; }
  INHERIT_TO_STRING_KV("ObMergerBasic", ObMergerBasic, K_(start_time), K_(force_flat_format));
protected:
  int prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx);
  int get_base_iter_curr_macro_block(const blocksstable::ObMacroBlockDesc *&macro_desc);
  void set_base_iter(const MERGE_ITER_ARRAY &minimum_iters) {
    const int64_t count = minimum_iters.count();
    if (!minimum_iters.empty() && minimum_iters[count - 1]->is_base_sstable_iter()
      && minimum_iters[count - 1]->is_macro_merge_iter()) {
      base_iter_ = minimum_iters[count - 1];
    } else {
      base_iter_ = nullptr;
    }
  }
  static const int64_t CACHED_TRANS_STATE_MAX_CNT = 10 * 1024l;
protected:
  virtual int inner_prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx) = 0;
  int close();
  virtual int inner_close() = 0;
protected:
  ObPartitionMergeIter *base_iter_;
  ObCachedTransStateMgr trans_state_mgr_;
  int64_t start_time_;
  bool force_flat_format_;
};

class ObPartitionMerger : public ObRowStoreMerger
{
public:
  ObPartitionMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  virtual ~ObPartitionMerger();
  virtual void reset();
  ObUncommitTxInfoCollector &get_uncommit_tx_info_collector() { return uncommit_tx_info_collector_; }
  ObMemUncommitTxInfo &get_uncommit_tx_info() { return uncommit_tx_info_collector_.uncommit_tx_info_; }
  INHERIT_TO_STRING_KV("ObPartitionMerger", ObRowStoreMerger, KPC_(merge_progress), K_(data_store_desc),
    K_(minimum_iters), KP_(validator));
protected:
  virtual int inner_process(const blocksstable::ObDatumRow &row, bool is_incremental_row = true) = 0;
  virtual int inner_close() override;
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) = 0;
  virtual int merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters, bool is_incremental_row = true) = 0;

  int process(const blocksstable::ObMicroBlock &micro_block, const int64_t sstable_idx);
  int process(
      const blocksstable::ObMacroBlockDesc &macro_meta,
      const ObMicroBlockData *micro_block_data,
      const int64_t sstable_idx);
  int process(const blocksstable::ObDatumRow &row, bool is_incremental_row = true);
  int merge_macro_block_iter(MERGE_ITER_ARRAY &minimum_iters, int64_t &reuse_row_cnt);
  int check_macro_block_op(const ObMacroBlockDesc &macro_desc, ObMacroBlockOp &block_op);
  int check_row_columns(const blocksstable::ObDatumRow &row);

private:
  int inner_open_macro_writer(ObBasicTabletMergeCtx &ctx, ObMergeParameter &merge_param);
  virtual int inner_prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx) override final;
  virtual int inner_init() = 0;
protected:
  static const int64_t DEFAULT_ITER_ARRAY_SIZE = DEFAULT_ITER_COUNT * sizeof(ObPartitionMergeIter *);
protected:
  ObPartitionMergeProgress *merge_progress_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  blocksstable::ObMacroBlockWriter *macro_writer_;
  MERGE_ITER_ARRAY minimum_iters_;
  ObProgressiveMergeHelper progressive_merge_helper_;
  ObIMacroBlockValidator *validator_;
  ObUncommitTxInfoCollector uncommit_tx_info_collector_;
  ObSEArray<ObSSTableMergeBlockInfo, 1> sstable_merge_block_info_array_;
};

class ObPartitionMajorMerger : public ObPartitionMerger
{
public:
  ObPartitionMajorMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  ~ObPartitionMajorMerger();
  virtual int merge_partition(
      ObBasicTabletMergeCtx &ctx,
      const int64_t idx) override;
  INHERIT_TO_STRING_KV("ObPartitionMajorMerger", ObPartitionMerger, "curr merger", "major merger");
protected:
  virtual int inner_process(const blocksstable::ObDatumRow &row, bool is_incremental_row = true) override;
private:
  virtual int inner_init() override;
  int init_progressive_merge_helper();
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) override;
  virtual int merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters, bool is_incremental_row = true) override;
  int merge_micro_block_iter(ObPartitionMergeIter &iter, int64_t &reuse_row_cnt);
  int reuse_base_sstable(ObPartitionMergeHelper &merge_helper);
  int reuse_base_small_sstable(ObPartitionMergeIter *base_iter, const MERGE_ITER_ARRAY &merge_iters);
  int inner_reuse_micro_or_row(ObPartitionMergeIter &base_iter, const MERGE_ITER_ARRAY &merge_iters);
};

class ObPartitionMinorMerger : public ObPartitionMerger
{
public:
  ObPartitionMinorMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  ~ObPartitionMinorMerger();
  virtual void reset() override;
  virtual int merge_partition(
      ObBasicTabletMergeCtx &ctx,
      const int64_t idx) override;
  INHERIT_TO_STRING_KV("ObPartitionMinorMerger", ObPartitionMerger, K_(minimum_iter_idxs));
protected:
  virtual int inner_process(const blocksstable::ObDatumRow &row, bool is_incremental_row = true) override;
  int find_minimum_iters_with_same_rowkey(MERGE_ITER_ARRAY &merge_iters,
                                          MERGE_ITER_ARRAY &minimum_iters,
                                          common::ObIArray<int64_t> &iter_idxs);
  virtual int merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters, bool is_incremental_row = true) override;
  int try_remove_ghost_iters(MERGE_ITER_ARRAY &merge_iters,
                             const bool shadow_already_output,
                             MERGE_ITER_ARRAY &minimum_iters,
                             common::ObIArray<int64_t> &iter_idxs);
  virtual int move_and_remove_unused_iters(MERGE_ITER_ARRAY &merge_iters,
                                           MERGE_ITER_ARRAY &minimum_iters,
                                           common::ObIArray<int64_t> &iter_idxs);
  int skip_shadow_row(MERGE_ITER_ARRAY &merge_iters);
  int check_need_prebuild_bloomfilter();
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) override;
private:
  virtual int inner_init() override;
  int init_progressive_merge_helper();
  int collect_merge_stat(
      const compaction::ObMergeType &merge_type,
      compaction::ObPartitionMinorMergeHelper &merge_helper,
      ObBasicTabletMergeCtx &ctx);
  int check_add_shadow_row(MERGE_ITER_ARRAY &merge_iters, const bool contain_multi_trans, bool &add_shadow_row);
  int merge_single_iter(ObPartitionMergeIter &merge_ite);
  int check_first_committed_row(const MERGE_ITER_ARRAY &merge_iters);
  int set_result_flag(MERGE_ITER_ARRAY &fuse_iters,
                      const bool rowkey_first_row,
                      const bool add_shadow_row,
                      const bool need_check_last);
  int move_and_remove_unused_iter(MERGE_ITER_ARRAY &merge_iters,
                                  ObPartitionMergeIter *merge_iter,
                                  int64_t iter_idx,
                                  bool &need_remove);
  int compact_delete_insert_iters(MERGE_ITER_ARRAY &merge_iters,
                                  MERGE_ITER_ARRAY &minimum_iters,
                                  const ObIArray<int64_t> &iter_idxs,
                                  bool need_add_shadow_row);

protected:
  common::ObSEArray<int64_t, DEFAULT_ITER_COUNT> minimum_iter_idxs_;
  common::ObArenaAllocator obj_copy_allocator_;
  storage::ObNopPos *nop_pos_[ObRowQueue::QI_MAX];
  blocksstable::ObRowQueue row_queue_;
};

class ObPartitionMergeDumper
{
public:
  static void print_error_info(const int err_no,
                               const MERGE_ITER_ARRAY &merge_iters,
                               ObBasicTabletMergeCtx &ctx);
  static int generate_dump_table_name(
      const char *dir_name,
      const storage::ObITable *table,
      char *file_name);
  static int judge_disk_free_space(const char *dir_name, storage::ObITable *table);

  static constexpr const double DUMP_TABLE_DISK_FREE_PERCENTAGE = 0.2;
  static constexpr const double MEMTABLE_DUMP_SIZE_PERCENTAGE = 0.2;
  static lib::ObMutex lock;
private:
  static bool need_dump_table(int err_no);
};

template <typename T>
int ObMergerBasic::alloc_row_writer(
    const int64_t cg_idx,
    const blocksstable::ObDatumRow &default_row,
    ObIArray<ObITable*> &tables,
    T *&writer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "merge_ctx_ is null", K(ret));
  } else {
    writer = nullptr;
    ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
    ObTabletMergeInfo **merge_infos = ctx->cg_merge_info_array_;
    const bool need_co_scan = ctx->is_build_row_store_from_rowkey_cg()
        || (ctx->is_build_redundant_row_store_from_rowkey_cg() && 0 == cg_idx);
    if (OB_UNLIKELY(nullptr == merge_infos || nullptr == merge_infos[cg_idx])) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected nullptr merge info", K(ret), K(merge_infos), K(cg_idx));
    } else if (OB_ISNULL(writer = OB_NEWx(T, &merger_arena_, need_co_scan))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for ObCOMergeWriter", K(ret));
    } else if (OB_FAIL(writer->init(*ctx, default_row, merge_param_, task_idx_,
                                    ctx->get_full_read_info(), cg_idx, *merge_infos[cg_idx], tables))) {
      // table->old major, read_info used to read old major
      STORAGE_LOG(WARN, "failed to init writer", K(ret), K(cg_idx), K(default_row), KPC(this));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(writer)) {
      writer->~T();
      merger_arena_.free(writer);
      writer = nullptr;
    }
  }
  return ret;
}
} //compaction
} //oceanbase


#endif /* OB_COMPACTION_PARTITION_MERGER_H_ */
