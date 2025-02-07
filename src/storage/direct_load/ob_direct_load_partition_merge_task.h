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
#pragma once

#include "lib/list/ob_dlist.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_external_scanner.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_scanner.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
} // namespace observer
namespace storage
{
class ObDirectLoadOriginTable;
class ObDirectLoadExternalTable;
class ObDirectLoadMultipleSSTable;
class ObDirectLoadMultipleHeapTable;
class ObDirectLoadSSTableConflictCheck;
class ObDirectLoadMultipleSSTableConflictCheck;
class ObDirectLoadInsertTabletContext;

class ObDirectLoadPartitionMergeTask : public common::ObDLinkBase<ObDirectLoadPartitionMergeTask>
{
public:
  ObDirectLoadPartitionMergeTask();
  virtual ~ObDirectLoadPartitionMergeTask();
  int process();
  int64_t get_row_count() const { return affected_rows_; }
  void stop();
  TO_STRING_KV(KPC_(merge_param), KPC_(merge_ctx), K_(parallel_idx));

protected:
  virtual int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                                  ObIAllocator &allocator) = 0;
  virtual int finish_check() { return OB_SUCCESS; }

private:
  int fill_sstable_slice(const int64_t slice_id,
                         const ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters);
  int fill_sstable_slice_batch(const int64_t slice_id,
                               const ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters);

protected:
  observer::ObTableLoadTableCtx *ctx_;
  const ObDirectLoadMergeParam *merge_param_;
  ObDirectLoadTabletMergeCtx *merge_ctx_;
  int64_t parallel_idx_;
  int64_t affected_rows_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  // 无主键表的hidden pk是在最后插入的时候才组装到datum_row的, 在这之前都无法调用handle_insert_row
  // 需要在最后插入的时候收集
  bool need_handle_dml_row_;
  bool is_stop_;
  bool is_inited_;
};

class ObDirectLoadPartitionEmptyMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionEmptyMergeTask() = default;
  virtual ~ObDirectLoadPartitionEmptyMergeTask() = default;
  int init(observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override
  {
    return OB_SUCCESS;
  }
};

class ObDirectLoadPartitionOriginDataMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionOriginDataMergeTask();
  virtual ~ObDirectLoadPartitionOriginDataMergeTask();
  int init(observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable *origin_table,
           const blocksstable::ObDatumRange &range,
           int64_t parallel_idx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;

private:
  ObDirectLoadOriginTable *origin_table_;
  const blocksstable::ObDatumRange *range_;
};

class ObDirectLoadPartitionRangeMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionRangeMergeTask();
  virtual ~ObDirectLoadPartitionRangeMergeTask();
  int init(observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadSSTable *> &sstable_array,
           const blocksstable::ObDatumRange &range,
           int64_t parallel_idx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;
  int finish_check() override;

private:
  ObDirectLoadOriginTable *origin_table_;
  const ObIArray<ObDirectLoadSSTable *> *sstable_array_;
  const blocksstable::ObDatumRange *range_;
  ObDirectLoadSSTableConflictCheck *conflict_check_;
};

class ObDirectLoadPartitionRangeMultipleMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionRangeMultipleMergeTask();
  virtual ~ObDirectLoadPartitionRangeMultipleMergeTask();
  int init(observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
           const blocksstable::ObDatumRange &range,
           int64_t parallel_idx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;
  int finish_check() override;

private:
  ObDirectLoadOriginTable *origin_table_;
  const ObIArray<ObDirectLoadMultipleSSTable *> *sstable_array_;
  const blocksstable::ObDatumRange *range_;
  ObDirectLoadMultipleSSTableConflictCheck *conflict_check_;
};

class ObDirectLoadPartitionHeapTableMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionHeapTableMergeTask();
  virtual ~ObDirectLoadPartitionHeapTableMergeTask();
  int init(observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadExternalTable *external_table,
           const share::ObTabletCacheInterval &pk_interval,
           int64_t parallel_idx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;

private:
  class RowIterator final : public ObDirectLoadIStoreRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(ObDirectLoadExternalTable *external_table,
             const ObDirectLoadTableDataDesc &table_data_desc,
             share::ObTabletCacheInterval &pk_interval);
    share::ObTabletCacheInterval *get_hide_pk_interval() const override { return pk_interval_; }
    int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
    INHERIT_TO_STRING_KV("IStoreRowIterator", ObDirectLoadIStoreRowIterator,
                         KPC_(pk_interval),
                         K_(is_inited));

  private:
    share::ObTabletCacheInterval *pk_interval_;
    ObDirectLoadExternalSequentialScanner<ObDirectLoadExternalRow> scanner_;
    blocksstable::ObDatumRow datum_row_;
    bool is_inited_;
  };

private:
  ObDirectLoadExternalTable *external_table_;
  share::ObTabletCacheInterval pk_interval_;
};

class ObDirectLoadPartitionHeapTableMultipleMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionHeapTableMultipleMergeTask();
  virtual ~ObDirectLoadPartitionHeapTableMultipleMergeTask();
  int init(observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadMultipleHeapTable *heap_table,
           const share::ObTabletCacheInterval &pk_interval,
           int64_t parallel_idx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;

private:
  class RowIterator final : public ObDirectLoadIStoreRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(ObDirectLoadMultipleHeapTable *heap_table,
             const common::ObTabletID &tablet_id,
             const ObDirectLoadTableDataDesc &table_data_desc,
             share::ObTabletCacheInterval &pk_interval);
    share::ObTabletCacheInterval *get_hide_pk_interval() const override { return pk_interval_; }
    int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
    INHERIT_TO_STRING_KV("IStoreRowIterator", ObDirectLoadIStoreRowIterator,
                         KPC_(pk_interval),
                         K_(is_inited));

  private:
    share::ObTabletCacheInterval *pk_interval_;
    ObDirectLoadMultipleHeapTableTabletWholeScanner scanner_;
    blocksstable::ObDatumRow datum_row_;
    bool is_inited_;
  };

private:
  ObDirectLoadMultipleHeapTable *heap_table_;
  share::ObTabletCacheInterval pk_interval_;
};

class ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask
  : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask();
  virtual ~ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask();
  int init(observer::ObTableLoadTableCtx *ctx,
           const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadMultipleHeapTable *> &heap_table_array,
           const share::ObTabletCacheInterval &pk_interval);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;

private:
  class RowIterator final : public ObDirectLoadIStoreRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(const common::ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array,
             const common::ObTabletID &tablet_id,
             const ObDirectLoadTableDataDesc &table_data_desc,
             share::ObTabletCacheInterval &pk_interval);
    share::ObTabletCacheInterval *get_hide_pk_interval() const override { return pk_interval_; }
    int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
    INHERIT_TO_STRING_KV("IStoreRowIterator", ObDirectLoadIStoreRowIterator,
                         K_(tablet_id),
                         KPC_(pk_interval),
                         K_(is_inited));

  private:
    int switch_next_heap_table();

  private:
    const common::ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array_;
    common::ObTabletID tablet_id_;
    ObDirectLoadTableDataDesc table_data_desc_;
    share::ObTabletCacheInterval *pk_interval_;
    ObDirectLoadMultipleHeapTableTabletWholeScanner scanner_;
    blocksstable::ObDatumRow datum_row_;
    int64_t pos_;
    bool is_inited_;
  };

private:
  ObDirectLoadOriginTable *origin_table_;
  const common::ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array_;
  share::ObTabletCacheInterval pk_interval_;
  blocksstable::ObDatumRange range_; // for origin iter
};

} // namespace storage
} // namespace oceanbase
