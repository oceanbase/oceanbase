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
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadSSTable;
class ObDirectLoadMultipleSSTable;

struct ObDirectLoadConflictCheckParam
{
public:
  ObDirectLoadConflictCheckParam();
  ~ObDirectLoadConflictCheckParam();
  bool is_valid() const;
  TO_STRING_KV(
      K(tablet_id_), K(store_column_count_), K(table_data_desc_), KP(origin_table_), KP(range_),
      KP(col_descs_), KP(lob_column_idxs_), KP(builder_), KP(datum_utils_), KP(dml_row_handler_));
public:
  common::ObTabletID tablet_id_;
  int64_t store_column_count_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadOriginTable *origin_table_;
  const blocksstable::ObDatumRange *range_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const common::ObArray<int64_t> *lob_column_idxs_;
  ObIDirectLoadPartitionTableBuilder *builder_;
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
      ObIStoreRowIterator *load_iter);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row);
private:
  int handle_get_next_row_finish(
      const ObDatumRow *load_row,
      const blocksstable::ObDatumRow *&datum_row);
  int compare(
      const blocksstable::ObDatumRow &first_row,
      const blocksstable::ObDatumRow &second_row,
      int &cmp_ret);
  int reopen_origin_iter(const ObDatumRow *datum_row);
  int handle_old_row(const ObDatumRow *old_row);
private:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator range_allocator_;
  ObDirectLoadConflictCheckParam param_;
  ObIStoreRowIterator *load_iter_;
  ObIStoreRowIterator *origin_iter_;
  const blocksstable::ObDatumRow *origin_row_;
  blocksstable::ObDatumRow append_row_;
  ObDatumRange new_range_;
  bool origin_iter_is_end_;
  bool is_inited_;
};

class ObDirectLoadSSTableConflictCheck : public ObIStoreRowIterator
{
public:
  ObDirectLoadSSTableConflictCheck();
  ~ObDirectLoadSSTableConflictCheck();
  int init(const ObDirectLoadConflictCheckParam &param,
           const common::ObIArray<ObDirectLoadSSTable *> &sstable_array);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
private:
  ObDirectLoadSSTableScanMerge scan_merge_;
  ObDirectLoadConflictCheck conflict_check_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableConflictCheck : public ObIStoreRowIterator
{
public:
  ObDirectLoadMultipleSSTableConflictCheck();
  virtual ~ObDirectLoadMultipleSSTableConflictCheck();
  int init(
      const ObDirectLoadConflictCheckParam &param,
      const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
private:
  ObDirectLoadMultipleDatumRange range_;
  ObDirectLoadMultipleSSTableScanMerge scan_merge_;
  ObDirectLoadConflictCheck conflict_check_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
