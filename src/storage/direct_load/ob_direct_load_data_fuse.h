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

#include "storage/blocksstable/ob_sstable.h"
#include "share/table/ob_table_load_define.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadSSTable;
class ObDirectLoadMultipleSSTable;

struct ObDirectLoadDataFuseParam
{
public:
  ObDirectLoadDataFuseParam();
  ~ObDirectLoadDataFuseParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(store_column_count), K_(table_data_desc), KP_(datum_utils),
               KP_(dml_row_handler));
public:
  common::ObTabletID tablet_id_;
  int64_t store_column_count_;
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
};

class ObDirectLoadDataFuse
{
public:
  ObDirectLoadDataFuse();
  virtual ~ObDirectLoadDataFuse();
  int init(const ObDirectLoadDataFuseParam &param, ObIStoreRowIterator *origin_iter,
           ObIStoreRowIterator *load_iter);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row);
protected:
  int supply_consume();
  int inner_get_next_row(const blocksstable::ObDatumRow *&datum_row);
protected:
  static const int64_t ITER_COUNT = 2;
  static const int64_t ORIGIN_IDX = 0;
  static const int64_t LOAD_IDX = 1;
  struct Item
  {
  public:
    const blocksstable::ObDatumRow *datum_row_;
    int64_t iter_idx_;
    TO_STRING_KV(K_(iter_idx), KPC_(datum_row));
  };
  class TwoRowsMerger
  {
  public:
    TwoRowsMerger();
    ~TwoRowsMerger();
    int init(int64_t rowkey_column_num, const blocksstable::ObStorageDatumUtils *datum_utils);
    int push(const Item &item);
    int top(const Item *&item);
    int pop();
    int rebuild();
    bool empty() const { return item_cnt_ == 0; }
    bool is_unique_champion() const { return is_unique_champion_; }
  private:
    int compare(const blocksstable::ObDatumRow &first_row,
                const blocksstable::ObDatumRow &second_row, int &cmp_ret);
  private:
    int64_t rowkey_column_num_;
    const blocksstable::ObStorageDatumUtils *datum_utils_;
    Item items_[2];
    int64_t item_cnt_;
    bool is_unique_champion_;
    bool is_inited_;
  };
protected:
  ObDirectLoadDataFuseParam param_;
  ObIStoreRowIterator *iters_[ITER_COUNT];
  TwoRowsMerger rows_merger_;
  int64_t consumers_[ITER_COUNT];
  int64_t consumer_cnt_;
  bool is_inited_;
};

class ObDirectLoadSSTableDataFuse : public ObIStoreRowIterator
{
public:
  ObDirectLoadSSTableDataFuse();
  ~ObDirectLoadSSTableDataFuse();
  int init(const ObDirectLoadDataFuseParam &param, ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadSSTable *> &sstable_array,
           const blocksstable::ObDatumRange &range);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
private:
  common::ObArenaAllocator allocator_;
  ObIStoreRowIterator *origin_iter_;
  ObDirectLoadSSTableScanMerge scan_merge_;
  ObDirectLoadDataFuse data_fuse_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableDataFuse : public ObIStoreRowIterator
{
public:
  ObDirectLoadMultipleSSTableDataFuse();
  ~ObDirectLoadMultipleSSTableDataFuse();
  int init(const ObDirectLoadDataFuseParam &param, ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
           const blocksstable::ObDatumRange &range);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadMultipleDatumRange range_;
  ObIStoreRowIterator *origin_iter_;
  ObDirectLoadMultipleSSTableScanMerge scan_merge_;
  ObDirectLoadDataFuse data_fuse_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
