/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_OB_COLUMN_ORIENTED_MERGER_H_
#define OB_STORAGE_OB_COLUMN_ORIENTED_MERGER_H_

#include "storage/compaction/ob_partition_merger.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/compaction/ob_partition_merge_iter.h"
#include "storage/blocksstable/ob_sstable.h"
#include "ob_co_merge_writer.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/column_store/ob_co_merge_ctx.h"
#include "storage/column_store/ob_co_merge_log_operator.h"
#include "share/compaction/ob_compaction_time_guard.h"

namespace oceanbase
{
namespace storage
{
class ObCOSSTableV2;
}

namespace compaction
{
class ObCOMinorSSTableMergeIter;
class ObCOMergeLogBuilder : public ObCOMergeLogIterator, public ObMerger
{
public:
  enum MoveNextOp : uint8_t {
    DO_NOTHING = 0,
    NEED_MOVE_NEXT,
    ONLY_REBUILD
  };
public:
  ObCOMergeLogBuilder(
      compaction::ObLocalArena &allocator,
      const ObStaticMergeParam &static_param,
      bool need_replay_base_cg = false)
    : ObMerger(allocator, static_param),
      need_move_minor_iter_(MoveNextOp::DO_NOTHING),
      need_move_major_iter_(MoveNextOp::DO_NOTHING),
      majors_merge_iter_(nullptr),
      cmp_(nullptr),
      need_replay_base_cg_(need_replay_base_cg),
      base_writer_(nullptr),
      time_guard_(),
      default_row_()
  {}
  virtual ~ObCOMergeLogBuilder() { reset(); };
  virtual int init(ObBasicTabletMergeCtx &ctx, const int64_t idx, const int64_t cg_idx) override;
  virtual void reset() override;
  virtual int get_next_log(ObMergeLog &mergelog, const ObMergeVectorStore *&vector_store, const blocksstable::ObDatumRow *&row) override;
  virtual int close() override;
  virtual int get_major_sstable_merge_iters_for_check(common::ObIArray<ObPartitionMergeIter *> &iters) override;
  VIRTUAL_TO_STRING_KV(KP_(cmp), K_(need_replay_base_cg), KP_(base_writer));
protected:
  int init_full_default_row();
  OB_INLINE const blocksstable::ObDatumRow *get_full_default_row() const { return default_row_.is_valid() ? &default_row_ : nullptr; }
protected:
  virtual OB_INLINE bool is_batch_merge_builder() const { return false; }
  virtual int alloc_base_writer(ObIArray<ObITable*> &tables);
  virtual void destroy_base_writer();
  template <typename T>
  int inner_alloc_base_writer(ObIArray<ObITable*> &tables);
  int inner_init();
  virtual int inner_get_next_log(
    ObMergeLog &mergelog,
    const ObMergeVectorStore *&vector_store,
    const blocksstable::ObDatumRow *&row);
  int init_majors_merge_helper();
  int handle_single_iter_end(
      ObMergeLog &mergelog,
      const blocksstable::ObDatumRow *&row);
  int build_merge_log(
      ObMergeLog::OpType op_type,
      ObPartitionMergeIter *row_store_iter,
      const blocksstable::ObDatumRow *row,
      ObMergeLog &mergelog,
      const bool replay_to_end);
  int check_with_filter(
      ObPartitionMergeIter *row_store_iter,
      const blocksstable::ObDatumRow *row,
      ObMergeLog::OpType &op_type,
      const bool replay_to_end);
  int check_range_with_filter(
      ObPartitionMergeIter *row_store_iter,
      ObMergeLog::OpType &op_type);
  int replay_base_cg(const ObMergeLog &merge_log, const ObMergeVectorStore *&vector_store, const blocksstable::ObDatumRow *&row, ObMergeIter *iter);
  virtual OB_INLINE void set_base_merge_iter(ObMergeIter *iter)
  { static_cast<ObCOMergeBaseRowWriter*>(base_writer_)->set_merge_iter(iter); }
  int compare(const blocksstable::ObDatumRow &left, ObPartitionMergeIter &row_store_iter, int64_t &cmp_ret);
  int move_iters_next(bool &is_single_iter_end);
  OB_INLINE void set_need_move_flag(const MoveNextOp need_move_minor_iter, const MoveNextOp need_move_major_iter)
  {
    need_move_minor_iter_ = need_move_minor_iter;
    need_move_major_iter_ = need_move_major_iter;
  }
protected:
  MoveNextOp need_move_minor_iter_;
  MoveNextOp need_move_major_iter_;
  ObMultiMajorMergeIter *majors_merge_iter_;
  ObPartitionMergeLoserTreeCmp *cmp_;
  bool need_replay_base_cg_;
  oceanbase::compaction::ObCOMergeRowWriter *base_writer_;
  ObCOMergeTimeGuard time_guard_;
  blocksstable::ObDatumRow default_row_;
};

class ObCOMinorSSTableMergeIter : public ObPartitionMergeHelper
{
public:
  ObCOMinorSSTableMergeIter(
      const ObITableReadInfo &read_info,
      ObIAllocator &allocator,
      ObIPartitionMergeFuser &fuser)
    : ObPartitionMergeHelper(read_info, allocator),
      partition_fuser_(fuser),
      curr_row_(nullptr),
      minimum_iters_()
  {}
  virtual ~ObCOMinorSSTableMergeIter() {}
  virtual ObPartitionMergeIter *alloc_merge_iter(
    const ObMergeParameter &merge_param,
    const int64_t iter_idx,
    const ObITable *table,
    ObCompactionFilterHandle &filter_handle) override
  {
    UNUSEDx(merge_param, iter_idx, table, filter_handle);
    return alloc_helper<ObPartitionRowMergeIter> (allocator_, allocator_);
  }
  virtual OB_INLINE bool need_skip_sstable(const bool table_need_full_merge, const ObITable &table) const { return table.is_major_type_sstable(); }
  OB_INLINE const blocksstable::ObDatumRow *get_curr_row() const { return curr_row_; }
  OB_INLINE bool is_single_iter() const { return minimum_iters_.count() == 1; }
  OB_INLINE ObPartitionMergeIter *get_top_iter() { return minimum_iters_.empty() ? nullptr : minimum_iters_.at(0); }
  OB_INLINE bool can_batch_scan()
  {
    ObPartitionMergeIter *top_iter = get_top_iter();
    return is_single_iter() && (top_iter != nullptr && top_iter->can_batch_scan_by_rowkey());
  }
  int get_curr_row(const blocksstable::ObDatumRow *&row);
  int next(const bool need_move_next);
protected:
  ObIPartitionMergeFuser &partition_fuser_;
  const blocksstable::ObDatumRow *curr_row_;
  MERGE_ITER_ARRAY minimum_iters_;
};

class ObCORowBatchMergeIter : public ObCOMinorSSTableMergeIter
{
public:
  ObCORowBatchMergeIter(
      const ObITableReadInfo &read_info,
      ObIAllocator &allocator,
      ObIPartitionMergeFuser &fuser)
    : ObCOMinorSSTableMergeIter(read_info, allocator, fuser)
  {}
  virtual ObPartitionMergeIter *alloc_merge_iter(
      const ObMergeParameter &merge_param,
      const int64_t iter_idx,
      const ObITable *table,
      ObCompactionFilterHandle &filter_handle) override
  {
    UNUSEDx(merge_param, iter_idx, filter_handle);
    const bool need_co_sstable_scan = table != nullptr && table->is_co_sstable() && static_cast<const ObCOSSTableV2*>(table)->is_rowkey_cg_base();
    return alloc_helper<ObPartitionRowMergeIter> (allocator_, allocator_, need_co_sstable_scan);
  }
  virtual OB_INLINE bool need_skip_sstable(const bool table_need_full_merge, const ObITable &table) const override
  {
    return table.is_major_type_sstable() && !table_need_full_merge;
  }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override;
};

typedef oceanbase::common::ObSEArray<oceanbase::compaction::ObCOMergeWriter *, DEFAULT_CG_MERGE_BATCH_SIZE, common::ObIAllocator&> MERGEWRITERS;
typedef oceanbase::common::ObSEArray<oceanbase::compaction::ObCOMergeLogIterator *, DEFAULT_CG_MERGE_BATCH_SIZE, common::ObIAllocator&> MERGE_LOG_ITERS;
class ObCOMergeLogReplayer : public ObMergerBasic
{
  class ObCOMergeLogReplayerCallback
  {
  public:
    ObCOMergeLogReplayerCallback(MERGEWRITERS &merge_writers, const int64_t replayed_idx, ObCOMergeTimeGuard &time_guard)
      : merge_writers_(merge_writers),
        replayed_idx_(replayed_idx),
        time_guard_(time_guard)
    {}
    int consume(const ObMergeLog &log, const ObMergeVectorStore *vector_store, const blocksstable::ObDatumRow *row);
  private:
    MERGEWRITERS &merge_writers_;
    const int64_t replayed_idx_;
    ObCOMergeTimeGuard &time_guard_;
  };
public:
  ObCOMergeLogReplayer(
      compaction::ObLocalArena &allocator,
      const ObStaticMergeParam &static_param,
      const uint32_t start_cg_idx,
      const uint32_t end_cg_idx,
      const bool use_row_to_build_column = false);
  virtual ~ObCOMergeLogReplayer() { reset(); };
  virtual void reset() override;
  virtual int init(ObBasicTabletMergeCtx &ctx, const int64_t idx);
  virtual int replay_merge_log();
  virtual OB_INLINE bool is_batch_merge_replayer() const { return false; }
  INHERIT_TO_STRING_KV("ObCOMergeLogReplayer", ObMergerBasic, K_(start_cg_idx), K_(end_cg_idx), K_(use_row_to_build_column));

protected:
  virtual int alloc_writers(
    const blocksstable::ObDatumRow &default_row,
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
    ObTabletMergeInfo **merge_infos,
    ObIArray<ObITable*> &tables);
  template <typename T>
  int alloc_row_writers(
      const blocksstable::ObDatumRow &default_row,
      ObTabletMergeInfo **merge_infos,
      ObIArray<ObITable*> &tables);

private:
  int close();
  int inner_init();
  int init_mergelog_iter(ObBasicTabletMergeCtx &ctx);
  int init_cg_writers(ObIArray<ObITable*> &tables);
  int alloc_single_writer(
      const blocksstable::ObDatumRow &default_row,
      const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
      ObTabletMergeInfo **merge_infos,
      ObIArray<ObITable*> &tables);
private:
  bool is_inited_;
  ObCOMergeLogIterator *mergelog_iter_;
  MERGEWRITERS merge_writers_;
  const uint64_t start_cg_idx_;
  const uint64_t end_cg_idx_;
  bool use_row_to_build_column_;
  bool need_replay_base_cg_;
  ObCachedTransStateMgr trans_state_mgr_;
  ObCOMergeTimeGuard time_guard_; // TODO(@huhaosheng.hhs): remove time guard
};

class ObCOMergeLogPersister
{
  class ObCOMergeLogPersisterCallback
  {
  public:
    ObCOMergeLogPersisterCallback(ObCOMergeLogFileWriter &mergelog_writer, ObCOMergeTimeGuard &time_guard)
      : mergelog_writer_(mergelog_writer),
        time_guard_(time_guard)
    {}
    int consume(const ObMergeLog &log, const ObMergeVectorStore *vector_store, const blocksstable::ObDatumRow *row);
  private:
    ObCOMergeLogFileWriter &mergelog_writer_;
    ObCOMergeTimeGuard &time_guard_;
  };
public:
  ObCOMergeLogPersister(compaction::ObLocalArena &allocator)
    : is_inited_(false),
      merger_arena_(allocator),
      mergelog_iter_(nullptr),
      mergelog_writer_(),
      time_guard_()
  {}
  virtual ~ObCOMergeLogPersister() { reset(); }
  void reset();
  int init(ObBasicTabletMergeCtx &ctx, const int64_t idx);
  int persist_merge_log();
  TO_STRING_KV(K_(is_inited), K_(mergelog_iter), K_(mergelog_writer));
private:
  int init_mergelog_iter(ObBasicTabletMergeCtx &ctx, const int64_t idx);
private:
  bool is_inited_;
  compaction::ObLocalArena &merger_arena_;
  ObCOMergeLogIterator *mergelog_iter_;
  ObCOMergeLogFileWriter mergelog_writer_;
  ObCOMergeTimeGuard time_guard_;
};

} //compaction
} //oceanbase


#endif
