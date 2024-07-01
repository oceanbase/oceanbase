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
#include "storage/direct_load/ob_direct_load_data_fuse.h"
#include "storage/direct_load/ob_direct_load_external_scanner.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_scanner.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"
#include "storage/direct_load/ob_direct_load_lob_builder.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace blocksstable
{
class ObDatumRange;
} // namespace blocksstable
namespace table
{
class ObTableLoadSqlStatistics;
} // namespace table
namespace storage
{
class ObDirectLoadOriginTable;
class ObDirectLoadExternalTable;
class ObDirectLoadMultipleSSTable;
class ObDirectLoadMultipleHeapTable;

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
  virtual int construct_row_iter(common::ObIAllocator &allocator,
                                 ObIStoreRowIterator *&row_iter) = 0;
protected:
  observer::ObTableLoadTableCtx *ctx_;
  const ObDirectLoadMergeParam *merge_param_;
  ObDirectLoadTabletMergeCtx *merge_ctx_;
  int64_t parallel_idx_;
  int64_t affected_rows_;
  ObDirectLoadLobBuilder lob_builder_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  table::ObTableLoadSqlStatistics *sql_statistics_;
  bool is_stop_;
  bool is_inited_;
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
  int construct_row_iter(common::ObIAllocator &allocator, ObIStoreRowIterator *&row_iter) override;
private:
  class RowIterator : public ObDirectLoadInsertTableRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(const ObDirectLoadMergeParam &merge_param,
             ObDirectLoadTabletMergeCtx *merge_ctx,
             ObDirectLoadOriginTable *origin_table,
             table::ObTableLoadSqlStatistics *sql_statistics,
             ObDirectLoadLobBuilder &lob_builder,
             const common::ObIArray<ObDirectLoadSSTable *> &sstable_array,
             const blocksstable::ObDatumRange &range,
             ObDirectLoadInsertTabletContext *insert_tablet_ctx);
    int inner_get_next_row(blocksstable::ObDatumRow *&datum_row) override;
  private:
    ObIStoreRowIterator *data_iter_;
    blocksstable::ObDatumRow datum_row_;
    int64_t rowkey_column_num_;
  };
private:
  ObDirectLoadOriginTable *origin_table_;
  const ObIArray<ObDirectLoadSSTable *> *sstable_array_;
  const blocksstable::ObDatumRange *range_;
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
  int construct_row_iter(common::ObIAllocator &allocator, ObIStoreRowIterator *&row_iter) override;
private:
  class RowIterator : public ObDirectLoadInsertTableRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(const ObDirectLoadMergeParam &merge_param,
             ObDirectLoadTabletMergeCtx *merge_ctx,
             ObDirectLoadOriginTable *origin_table,
             table::ObTableLoadSqlStatistics *sql_statistics,
             ObDirectLoadLobBuilder &lob_builder,
             const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
             const blocksstable::ObDatumRange &range,
             ObDirectLoadInsertTabletContext *insert_tablet_ctx);
    int inner_get_next_row(blocksstable::ObDatumRow *&datum_row) override;
  private:
    ObIStoreRowIterator *data_iter_;
    blocksstable::ObDatumRow datum_row_;
    int64_t rowkey_column_num_;
  };
private:
  ObDirectLoadOriginTable *origin_table_;
  const ObIArray<ObDirectLoadMultipleSSTable *> *sstable_array_;
  const blocksstable::ObDatumRange *range_;
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
  int construct_row_iter(common::ObIAllocator &allocator, ObIStoreRowIterator *&row_iter) override;
private:
  class RowIterator : public ObDirectLoadInsertTableRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(const ObDirectLoadMergeParam &merge_param,
             const common::ObTabletID &tablet_id,
             ObDirectLoadExternalTable *external_table,
             table::ObTableLoadSqlStatistics *sql_statistics,
             ObDirectLoadLobBuilder &lob_builder,
             const share::ObTabletCacheInterval &pk_interval,
             ObDirectLoadInsertTabletContext *insert_tablet_ctx);
    int inner_get_next_row(blocksstable::ObDatumRow *&datum_row) override;
  private:
    ObDirectLoadExternalSequentialScanner<ObDirectLoadExternalRow> scanner_;
    blocksstable::ObDatumRow datum_row_;
    blocksstable::ObStorageDatum *deserialize_datums_;
    int64_t deserialize_datum_cnt_;
    share::ObTabletCacheInterval pk_interval_;
    ObDirectLoadDMLRowHandler *dml_row_handler_;
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
  int construct_row_iter(common::ObIAllocator &allocator, ObIStoreRowIterator *&row_iter) override;
private:
  class RowIterator : public ObDirectLoadInsertTableRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(const ObDirectLoadMergeParam &merge_param,
             const common::ObTabletID &tablet_id,
             ObDirectLoadMultipleHeapTable *heap_table,
             table::ObTableLoadSqlStatistics *sql_statistics,
             ObDirectLoadLobBuilder &lob_builder,
             const share::ObTabletCacheInterval &pk_interval,
             ObDirectLoadInsertTabletContext *insert_tablet_ctx);
    int inner_get_next_row(blocksstable::ObDatumRow *&datum_row) override;
  private:
    ObDirectLoadMultipleHeapTableTabletWholeScanner scanner_;
    blocksstable::ObDatumRow datum_row_;
    blocksstable::ObStorageDatum *deserialize_datums_;
    int64_t deserialize_datum_cnt_;
    share::ObTabletCacheInterval pk_interval_;
    ObDirectLoadDMLRowHandler *dml_row_handler_;
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
           const ObDirectLoadMergeParam &merge_param, ObDirectLoadTabletMergeCtx *merge_ctx,
           ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadMultipleHeapTable *> &heap_table_array,
           const share::ObTabletCacheInterval &pk_interval);
protected:
  int construct_row_iter(common::ObIAllocator &allocator, ObIStoreRowIterator *&row_iter) override;
private:
  class RowIterator : public ObDirectLoadInsertTableRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(const ObDirectLoadMergeParam &merge_param,
             ObDirectLoadTabletMergeCtx *merge_ctx,
             ObDirectLoadOriginTable *origin_table,
             table::ObTableLoadSqlStatistics *sql_statistics,
             ObDirectLoadLobBuilder &lob_builder,
             const common::ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array,
             const share::ObTabletCacheInterval &pk_interval,
             ObDirectLoadInsertTabletContext *insert_tablet_ctx);
    int inner_get_next_row(blocksstable::ObDatumRow *&datum_row) override;
  private:
    int switch_next_heap_table();
  private:
    // for iter origin table
    common::ObArenaAllocator allocator_;
    blocksstable::ObDatumRange range_;
    ObIStoreRowIterator *origin_iter_;
    int64_t rowkey_column_num_;
    int64_t store_column_count_;
    // for iter multiple heap table
    common::ObTabletID tablet_id_;
    ObDirectLoadTableDataDesc table_data_desc_;
    const common::ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array_;
    int64_t pos_;
    ObDirectLoadMultipleHeapTableTabletWholeScanner scanner_;
    blocksstable::ObDatumRow datum_row_;
    blocksstable::ObStorageDatum *deserialize_datums_;
    int64_t deserialize_datum_cnt_;
    share::ObTabletCacheInterval pk_interval_;
    ObDirectLoadDMLRowHandler *dml_row_handler_;
  };
private:
  ObDirectLoadOriginTable *origin_table_;
  const common::ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array_;
  share::ObTabletCacheInterval pk_interval_;
};

} // namespace storage
} // namespace oceanbase
