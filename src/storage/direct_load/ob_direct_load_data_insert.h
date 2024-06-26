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
#include "storage/direct_load/ob_direct_load_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadSSTable;
class ObDirectLoadMultipleSSTable;

struct ObDirectLoadDataInsertParam
{
public:
  ObDirectLoadDataInsertParam();
  ~ObDirectLoadDataInsertParam();
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

class ObDirectLoadDataInsert
{
public:
  ObDirectLoadDataInsert();
  ~ObDirectLoadDataInsert();
  int init(
      const ObDirectLoadDataInsertParam &param,
      ObIStoreRowIterator *load_iter);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row);
private:
  ObDirectLoadDataInsertParam param_;
  ObIStoreRowIterator *load_iter_;
  bool is_inited_;
};

class ObDirectLoadSSTableDataInsert : public ObIStoreRowIterator
{
public:
  ObDirectLoadSSTableDataInsert();
  ~ObDirectLoadSSTableDataInsert();
  int init(
      const ObDirectLoadDataInsertParam &param,
      const common::ObIArray<ObDirectLoadSSTable *> &sstable_array,
      const blocksstable::ObDatumRange &range);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row);
private:
  ObDirectLoadSSTableScanMerge scan_merge_;
  ObDirectLoadDataInsert data_insert_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableDataInsert : public ObIStoreRowIterator
{
public:
  ObDirectLoadMultipleSSTableDataInsert();
  ~ObDirectLoadMultipleSSTableDataInsert();
  int init(
      const ObDirectLoadDataInsertParam &param,
      const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
      const blocksstable::ObDatumRange &range);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row);
private:
  ObDirectLoadMultipleDatumRange range_;
  ObDirectLoadMultipleSSTableScanMerge scan_merge_;
  ObDirectLoadDataInsert data_insert_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
