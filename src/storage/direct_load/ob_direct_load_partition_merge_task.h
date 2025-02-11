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
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_external_scanner.h"
#include "storage/direct_load/ob_direct_load_i_merge_task.h"
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
class ObDirectLoadInsertTabletContext;

class ObDirectLoadPartitionMergeTask : public ObDirectLoadIMergeTask
{
public:
  ObDirectLoadPartitionMergeTask();
  virtual ~ObDirectLoadPartitionMergeTask();
  int process() override;
  void stop() override;
  ObDirectLoadTabletMergeCtx *get_merge_ctx() override { return merge_ctx_; }
  TO_STRING_KV(KPC_(merge_param), KPC_(merge_ctx), K_(parallel_idx));

protected:
  int inner_init(ObDirectLoadTabletMergeCtx *merge_ctx,
                 int64_t parallel_idx,
                 bool need_handle_dml_row = false);
  virtual int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                                  ObIAllocator &allocator) = 0;

private:
  int fill_sstable_slice(const int64_t slice_id,
                         const ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters);
  int fill_sstable_slice_batch(const int64_t slice_id,
                               const ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters);

protected:
  observer::ObTableLoadTableCtx *ctx_;
  const ObDirectLoadMergeParam *merge_param_;
  ObDirectLoadTabletMergeCtx *merge_ctx_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  int64_t parallel_idx_;
  int64_t affected_rows_;
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
  int init(ObDirectLoadTabletMergeCtx *merge_ctx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override
  {
    row_iters.reset();
    return OB_SUCCESS;
  }
};

class ObDirectLoadPartitionOriginDataMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionOriginDataMergeTask();
  virtual ~ObDirectLoadPartitionOriginDataMergeTask();
  int init(ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable &origin_table,
           const blocksstable::ObDatumRange &range,
           int64_t parallel_idx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;

private:
  ObDirectLoadOriginTable *origin_table_;
  const blocksstable::ObDatumRange *range_;
};

// 堆表列存表unrescan场景, 对于已有数据也要重新分配主键值
class ObDirectLoadPartitionOriginDataUnrescanMergeTask : public ObDirectLoadIMergeTask
{
public:
  ObDirectLoadPartitionOriginDataUnrescanMergeTask();
  virtual ~ObDirectLoadPartitionOriginDataUnrescanMergeTask();
  int init(ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable &origin_table,
           const blocksstable::ObDatumRange &range,
           int64_t parallel_idx);
  int process() override;
  void stop() override;
  ObDirectLoadTabletMergeCtx *get_merge_ctx() override { return merge_ctx_; }
  TO_STRING_KV(KPC_(merge_param), KPC_(merge_ctx), K_(parallel_idx));
private:
  observer::ObTableLoadTableCtx *ctx_;
  const ObDirectLoadMergeParam *merge_param_;
  ObDirectLoadTabletMergeCtx *merge_ctx_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  ObDirectLoadOriginTable *origin_table_;
  const blocksstable::ObDatumRange *range_;
  int64_t parallel_idx_;
  int64_t affected_rows_;
  bool is_stop_;
  bool is_inited_;
};

class ObDirectLoadPartitionRangeMultipleMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionRangeMultipleMergeTask();
  virtual ~ObDirectLoadPartitionRangeMultipleMergeTask();
  int init(ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable &origin_table,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const ObDirectLoadTableHandleArray &sstable_array,
           const blocksstable::ObDatumRange &range,
           int64_t parallel_idx);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;

private:
  ObDirectLoadOriginTable *origin_table_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadTableHandleArray sstable_array_;
  const blocksstable::ObDatumRange *range_;
};

// 堆表按分区分开写extern_table, unused
class ObDirectLoadPartitionHeapTableMergeTask : public ObDirectLoadPartitionMergeTask
{
public:
  ObDirectLoadPartitionHeapTableMergeTask();
  virtual ~ObDirectLoadPartitionHeapTableMergeTask();
  int init(ObDirectLoadTabletMergeCtx *merge_ctx,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const ObDirectLoadTableHandle &external_table,
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
    int init(const ObDirectLoadTableHandle &external_table,
             const ObDirectLoadTableDataDesc &table_data_desc,
             share::ObTabletCacheInterval &pk_interval);
    share::ObTabletCacheInterval *get_hide_pk_interval() const override { return pk_interval_; }
    int get_next_row(const ObDirectLoadDatumRow *&datum_row) override;
    INHERIT_TO_STRING_KV("IStoreRowIterator", ObDirectLoadIStoreRowIterator,
                         KPC_(pk_interval),
                         K_(is_inited));

  private:
    share::ObTabletCacheInterval *pk_interval_;
    ObDirectLoadExternalSequentialScanner<ObDirectLoadExternalRow> scanner_;
    ObDirectLoadDatumRow datum_row_;
    bool is_inited_;
  };

private:
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadTableHandle external_table_;
  share::ObTabletCacheInterval pk_interval_;
};

class ObDirectLoadPartitionHeapTableMultipleMergeTask : public ObDirectLoadPartitionMergeTask
{
  friend class ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask;
public:
  ObDirectLoadPartitionHeapTableMultipleMergeTask();
  virtual ~ObDirectLoadPartitionHeapTableMultipleMergeTask();
  int init(ObDirectLoadTabletMergeCtx *merge_ctx,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const ObDirectLoadTableHandle &heap_table,
           const share::ObTabletCacheInterval &pk_interval,
           int64_t parallel_idx);
  int init(ObDirectLoadTabletMergeCtx *merge_ctx,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const ObDirectLoadTableHandleArray &heap_table_array,
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
    int init(const ObDirectLoadTableHandleArray &heap_table_array,
             const common::ObTabletID &tablet_id,
             const ObDirectLoadTableDataDesc &table_data_desc,
             share::ObTabletCacheInterval &pk_interval);
    share::ObTabletCacheInterval *get_hide_pk_interval() const override { return pk_interval_; }
    int get_next_row(const ObDirectLoadDatumRow *&datum_row) override;
    INHERIT_TO_STRING_KV("IStoreRowIterator", ObDirectLoadIStoreRowIterator,
                         K_(tablet_id),
                         KPC_(pk_interval),
                         K_(is_inited));

  private:
    int switch_next_heap_table();

  private:
    const ObDirectLoadTableHandleArray *heap_table_array_;
    common::ObTabletID tablet_id_;
    ObDirectLoadTableDataDesc table_data_desc_;
    share::ObTabletCacheInterval *pk_interval_;
    ObDirectLoadMultipleHeapTableTabletWholeScanner scanner_;
    ObDirectLoadDatumRow datum_row_;
    int64_t pos_;
    bool is_inited_;
  };

private:
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadTableHandleArray heap_table_array_;
  share::ObTabletCacheInterval pk_interval_;
};

class ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask
  : public ObDirectLoadPartitionMergeTask
{
  typedef ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator RowIterator;
public:
  ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask();
  virtual ~ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask();
  int init(ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable &origin_table,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const ObDirectLoadTableHandleArray &heap_table_array,
           const share::ObTabletCacheInterval &pk_interval);

protected:
  int construct_row_iters(ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
                          ObIAllocator &allocator) override;

private:
  ObDirectLoadOriginTable *origin_table_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadTableHandleArray heap_table_array_;
  share::ObTabletCacheInterval pk_interval_;
  blocksstable::ObDatumRange whole_range_; // for origin iter
};

} // namespace storage
} // namespace oceanbase
