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

#include "lib/list/ob_dlist.h"
#include "storage/direct_load/ob_direct_load_lob_meta_row_iter.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertTabletContext;

class ObDirectLoadPartitionDelLobTask : public common::ObDLinkBase<ObDirectLoadPartitionDelLobTask>
{
public:
  ObDirectLoadPartitionDelLobTask();
  ~ObDirectLoadPartitionDelLobTask();
  int init(const ObDirectLoadMergeParam &merge_param, ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
           const blocksstable::ObDatumRange &range, int64_t parallel_idx);
  int process();
  void stop();
  TO_STRING_KV(K_(parallel_idx), K_(is_stop));

private:
  class RowIterator : public ObIStoreRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(const ObDirectLoadMergeParam &merge_param, const ObDirectLoadTabletMergeCtx *merge_ctx,
             ObDirectLoadOriginTable *origin_table,
             const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
             const blocksstable::ObDatumRange &range, int64_t parallel_idx,
             ObDirectLoadInsertTabletContext *insert_tablet_ctx);
    int get_next_row(const blocksstable::ObDatumRow *&result_row) override;

  private:
    ObDirectLoadLobMetaRowIter lob_iter_;
    blocksstable::ObDatumRow datum_row_;
    bool is_inited_;
  };

private:
  const ObDirectLoadMergeParam *merge_param_;
  ObDirectLoadTabletMergeCtx *merge_ctx_;
  ObDirectLoadOriginTable *origin_table_;
  const ObIArray<ObDirectLoadMultipleSSTable *> *sstable_array_;
  const blocksstable::ObDatumRange *range_;
  int64_t parallel_idx_;
  bool is_stop_;
  bool is_inited_;
};

class ObDirectLoadDelLobTaskIterator
{
public:
  ObDirectLoadDelLobTaskIterator();
  ~ObDirectLoadDelLobTaskIterator();
  int init(storage::ObDirectLoadMergeCtx *merge_ctx);
  int get_next_task(ObDirectLoadPartitionDelLobTask *&task);

private:
  storage::ObDirectLoadMergeCtx *merge_ctx_;
  storage::ObDirectLoadTabletMergeCtx *tablet_merge_ctx_;
  int64_t tablet_pos_;
  int64_t task_pos_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
