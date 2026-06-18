/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_COMPACTION_VECTORIZATION_OB_COMPACTION_BATCH_MERGER_H_
#define OB_STORAGE_COMPACTION_VECTORIZATION_OB_COMPACTION_BATCH_MERGER_H_

#include "storage/column_store/ob_column_oriented_merger.h"
#include "storage/compaction/ob_partition_merger.h"
#include "ob_co_batch_merge_writer.h"

namespace oceanbase
{
namespace compaction
{
class ObPartitionMajorBatchMerger : public ObPartitionMajorMerger
{
public:
  ObPartitionMajorBatchMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  virtual ~ObPartitionMajorBatchMerger();
  virtual void reset() override;
protected:
  virtual int inner_init() override;
  virtual int inner_close() override;
  virtual int inner_process(const blocksstable::ObDatumRow &row, const bool is_incremental_row = true) override;
  virtual int merge_batch_rows(MERGE_ITER_ARRAY &merge_iters) override;
  int flush_write_store();
  int flush_read_store(const bool is_incremental_row = false);
  virtual int process(const blocksstable::ObMicroBlock &micro_block, const int64_t sstable_idx) override;
  virtual int process(
      const blocksstable::ObMacroBlockDesc &macro_meta,
      const ObMicroBlockData *micro_block_data,
      const int64_t sstable_idx) override;
private:
  ObMergeVectorStorePair stores_;
};

class ObCOBatchMergeLogBuilder : public ObCOMergeLogBuilder
{
public:
  ObCOBatchMergeLogBuilder(
      compaction::ObLocalArena &allocator,
      const ObStaticMergeParam &static_param,
      const bool need_replay_base_cg = false)
    : ObCOMergeLogBuilder(allocator, static_param, need_replay_base_cg),
      read_store_(),
      batch_scan_iter_(nullptr),
      border_rowkey_()
  {}
  virtual ~ObCOBatchMergeLogBuilder() { reset(); }
  virtual void reset() override;
  virtual int init(ObBasicTabletMergeCtx &ctx, const int64_t idx, const int64_t cg_idx) override;
protected:
  virtual int inner_get_next_log(ObMergeLog &mergelog, const ObMergeVectorStore *&vector_store, const blocksstable::ObDatumRow *&row) override;
  virtual OB_INLINE bool is_batch_merge_builder() const override { return true; }
  virtual int alloc_base_writer(ObIArray<ObITable*> &tables) override;
  virtual OB_INLINE void set_base_merge_iter(ObMergeIter *iter) override
  { static_cast<ObCOBatchMergeBaseRowWriter*>(base_writer_)->set_merge_iter(iter); }
private:
  int get_next_batch_log(ObMergeLog &mergelog, const ObMergeVectorStore *&vector_store);
  OB_INLINE bool can_batch_scan() const { return nullptr != batch_scan_iter_; }
  int calculate_border_rowkey(ObMergeLog &mergelog,
                              const ObMergeVectorStore *&vector_store,
                              ObPartitionMergeIter *winner_iter,
                              ObPartitionMergeHelper *winner_helper,
                              ObPartitionMergeIter *loser_iter);
private:
  ObMergeVectorStore read_store_; // read buffer // save full row
  ObPartitionMergeIter *batch_scan_iter_;
  blocksstable::ObDatumRowkey border_rowkey_;
};

class ObCOBatchMergeLogReplayer : public ObCOMergeLogReplayer
{
public:
  ObCOBatchMergeLogReplayer(
      compaction::ObLocalArena &allocator,
      const ObStaticMergeParam &static_param,
      const uint32_t start_cg_idx,
      const uint32_t end_cg_idx)
    : ObCOMergeLogReplayer(allocator, static_param, start_cg_idx, end_cg_idx)
  {}

  virtual OB_INLINE bool is_batch_merge_replayer() const override { return true; }
protected:
  virtual int alloc_writers(
    const blocksstable::ObDatumRow &default_row,
    ObTabletMergeInfo **merge_infos,
    ObIArray<ObITable*> &tables) override;
};

}
}
#endif