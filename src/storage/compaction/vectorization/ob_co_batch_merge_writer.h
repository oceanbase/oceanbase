/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OB_STORAGE_COMPACTION_VECTORIZATION_OB_CO_BATCH_MERGE_WRITER_H_
#define OB_STORAGE_COMPACTION_VECTORIZATION_OB_CO_BATCH_MERGE_WRITER_H_

#include "storage/column_store/ob_co_merge_writer.h"

namespace oceanbase
{
namespace compaction
{
class ObCOBatchMergeRowWriter : public ObCOMergeRowWriter
{
public:
  ObCOBatchMergeRowWriter(bool need_co_scan = false)
  : ObCOMergeRowWriter(need_co_scan),
    stores_()
  {}
  virtual ~ObCOBatchMergeRowWriter();
  virtual int end_write(ObCOTabletMergeCtx &co_ctx) override;
protected:
  virtual int inner_init(ObBasicTabletMergeCtx &ctx) override;
  virtual int flush_pending_buffered_rows() override;
  virtual int replay_batch_mergelog(
      const ObMergeLog &mergelog,
      const ObMergeVectorStore &vector_store,
      const bool need_check_project,
      const bool need_check_filter) override;
  virtual int replay_range_mergelog(const ObMergeLog &mergelog) override;
  virtual OB_INLINE bool is_batch_merge_writer() const override { return true; }
  virtual ObMergeVectorStore *get_read_vector_store() override { return &stores_.read_store();}
  int process_batch_rows();
private:
  virtual int process(ObCOMajorMergeIter *iter,
                      const ObMacroBlockDesc &macro_desc,
                      const ObMicroBlockData *micro_block_data) override;
  virtual int process(const blocksstable::ObMicroBlock &micro_block,
                      const int64_t sstable_idx,
                      ObMergeVectorStore *read_vector_store = nullptr) override;
  virtual int process(const blocksstable::ObDatumRow &row) override;
protected:
  ObMergeVectorStorePair stores_;
  ObCompactionFilterHandle filter_handle_;
};

// need to use process of ObCOBatchMergeRowWriter
class ObCOBatchMergeBaseRowWriter : public ObCOBatchMergeRowWriter
{
public:
  ObCOBatchMergeBaseRowWriter(bool need_co_scan = false)
  : ObCOBatchMergeRowWriter(need_co_scan),
    merge_iter_(nullptr, false, allocator_),
    border_key_()
  {}
  virtual ~ObCOBatchMergeBaseRowWriter()
  {
    merge_iter_.iter_ = nullptr;
  }
  void set_merge_iter(ObMergeIter *iter) { merge_iter_.iter_ = iter; }
protected:
  virtual int replay_single_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row) override;
  virtual int replay_range_mergelog(const ObMergeLog &mergelog) override;
  virtual OB_INLINE bool is_base_cg_writer() const override { return true; }
private:
  ObCOMajorMergeIter merge_iter_;
  ObDatumRowkey border_key_; // TODO: used to batch scan base cg
};
}
}
#endif