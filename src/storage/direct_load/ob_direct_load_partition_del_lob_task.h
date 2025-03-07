/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "storage/direct_load/ob_direct_load_i_merge_task.h"
#include "storage/direct_load/ob_direct_load_lob_meta_row_iter.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTabletMergeCtx;
class ObDirectLoadOriginTable;

class ObDirectLoadPartitionDelLobTask : public ObDirectLoadIMergeTask
{
public:
  ObDirectLoadPartitionDelLobTask();
  ~ObDirectLoadPartitionDelLobTask();
  int init(ObDirectLoadTabletMergeCtx *merge_ctx, ObDirectLoadOriginTable &origin_table,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const ObDirectLoadTableHandleArray &sstable_array,
           const blocksstable::ObDatumRange &range, const blocksstable::ObMacroDataSeq &data_seq,
           const int64_t parallel_idx);
  int process() override;
  void stop() override;
  ObDirectLoadTabletMergeCtx *get_merge_ctx() override { return merge_ctx_; }
  TO_STRING_KV(KP_(merge_ctx), K_(sstable_array), KPC_(range), K_(data_seq), K_(parallel_idx));

private:
  class RowIterator : public ObIStoreRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(ObDirectLoadTabletMergeCtx *merge_ctx, ObDirectLoadOriginTable &origin_table,
             const ObDirectLoadTableDataDesc &table_data_desc,
             const ObDirectLoadTableHandleArray &sstable_array,
             const blocksstable::ObDatumRange &range, int64_t parallel_idx);
    int get_next_row(const blocksstable::ObDatumRow *&result_row) override;

  private:
    ObDirectLoadLobMetaRowIter lob_iter_;
    blocksstable::ObDatumRow datum_row_;
    bool is_inited_;
  };

private:
  ObDirectLoadTabletMergeCtx *merge_ctx_;
  ObDirectLoadOriginTable *origin_table_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadTableHandleArray sstable_array_;
  const blocksstable::ObDatumRange *range_;
  blocksstable::ObMacroDataSeq data_seq_;
  int64_t parallel_idx_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
