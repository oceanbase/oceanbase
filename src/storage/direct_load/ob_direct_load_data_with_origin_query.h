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

#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDMLRowHandler;
class ObDirectLoadOriginTable;
class ObDirectLoadOriginTableGetter;

struct ObDirectLoadDataWithOriginQueryParam
{
public:
  ObDirectLoadDataWithOriginQueryParam();
  ~ObDirectLoadDataWithOriginQueryParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(rowkey_count), K_(store_column_count), K_(table_data_desc),
               KP_(origin_table), KP_(col_descs), KP_(datum_utils), KP_(dml_row_handler));

public:
  common::ObTabletID tablet_id_;
  uint64_t rowkey_count_;//output data desc
  uint64_t store_column_count_;//output data desc
  ObDirectLoadTableDataDesc table_data_desc_;//input data desc
  ObDirectLoadOriginTable *origin_table_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
};

class ObDirectLoadDataWithOriginQuery
{
public:
  ObDirectLoadDataWithOriginQuery();
  virtual ~ObDirectLoadDataWithOriginQuery();
  int init(const ObDirectLoadDataWithOriginQueryParam &param,
           ObDirectLoadIStoreRowIterator *load_iter);
  int get_next_row(const ObDirectLoadDatumRow *&row);

private:
  int query_full_row(const ObDirectLoadDatumRow &rowkey_row, const ObDirectLoadDatumRow *&full_row);

private:
  ObArenaAllocator allocator_;
  ObArenaAllocator rowkey_allocator_;
  ObDirectLoadDataWithOriginQueryParam param_;
  ObDirectLoadIStoreRowIterator *load_iter_;
  ObDirectLoadOriginTableGetter *origin_getter_;
  blocksstable::ObDatumRowkey rowkey_;
  ObDirectLoadDatumRow datum_row_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableDataWithOriginQuery final : public ObDirectLoadIStoreRowIterator
{
public:
  ObDirectLoadMultipleSSTableDataWithOriginQuery();
  ~ObDirectLoadMultipleSSTableDataWithOriginQuery();
  int init(const ObDirectLoadDataWithOriginQueryParam &param,
           const ObDirectLoadTableHandleArray &sstable_array,
           const blocksstable::ObDatumRange &range);
  int get_next_row(const ObDirectLoadDatumRow *&datum_row) override;

private:
  ObDirectLoadMultipleDatumRange range_;
  ObDirectLoadMultipleSSTableScanMerge scan_merge_;
  ObDirectLoadDataWithOriginQuery data_delete_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase