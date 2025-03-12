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

#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#include "storage/direct_load/ob_direct_load_sstable_scan_merge.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDMLRowHandler;
class ObDirectLoadOriginTable;
class ObDirectLoadOriginTableScanner;

struct ObDirectLoadConflictCheckParam
{
public:
  ObDirectLoadConflictCheckParam();
  ~ObDirectLoadConflictCheckParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id),
               K_(table_data_desc),
               KP_(origin_table),
               KPC_(range),
               KP_(col_descs),
               KP_(datum_utils),
               KP_(dml_row_handler));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadOriginTable *origin_table_;
  const blocksstable::ObDatumRange *range_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
};

class ObDirectLoadConflictCheck
{
public:
  static const int64_t SKIP_THESHOLD = 100;
  ObDirectLoadConflictCheck();
  virtual ~ObDirectLoadConflictCheck();
  int init(
      const ObDirectLoadConflictCheckParam &param,
      ObDirectLoadIStoreRowIterator *load_iter);
  int get_next_row(const ObDirectLoadDatumRow *&datum_row);
private:
  int handle_get_next_row_finish(
      const ObDirectLoadDatumRow *load_row,
      const ObDirectLoadDatumRow *&datum_row);
  int compare(
      const ObDirectLoadDatumRow &first_row,
      const ObDirectLoadDatumRow &second_row,
      int &cmp_ret);
  int reopen_origin_scanner(const ObDirectLoadDatumRow *datum_row);
private:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator range_allocator_;
  ObDirectLoadConflictCheckParam param_;
  ObDirectLoadIStoreRowIterator *load_iter_;
  ObDirectLoadOriginTableScanner *origin_scanner_;
  const ObDirectLoadDatumRow *origin_row_;
  ObDatumRange new_range_;
  bool origin_iter_is_end_;
  bool is_inited_;
};

class ObDirectLoadSSTableConflictCheck final : public ObDirectLoadIStoreRowIterator
{
public:
  ObDirectLoadSSTableConflictCheck();
  ~ObDirectLoadSSTableConflictCheck();
  int init(const ObDirectLoadConflictCheckParam &param,
           const ObDirectLoadTableHandleArray &sstable_array);
  int get_next_row(const ObDirectLoadDatumRow *&datum_row) override;
private:
  ObDirectLoadSSTableScanMerge scan_merge_;
  ObDirectLoadConflictCheck conflict_check_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableConflictCheck final : public ObDirectLoadIStoreRowIterator
{
public:
  ObDirectLoadMultipleSSTableConflictCheck();
  virtual ~ObDirectLoadMultipleSSTableConflictCheck();
  int init(
      const ObDirectLoadConflictCheckParam &param,
      const ObDirectLoadTableHandleArray &sstable_array);
  int get_next_row(const ObDirectLoadDatumRow *&datum_row) override;
private:
  ObDirectLoadMultipleDatumRange range_;
  ObDirectLoadMultipleSSTableScanMerge scan_merge_;
  ObDirectLoadConflictCheck conflict_check_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
