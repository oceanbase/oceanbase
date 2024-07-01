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

#include "storage/blocksstable/ob_datum_range.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMultipleSSTable;

class ObDirectLoadLobIdConflictHandler : public ObDirectLoadDMLRowHandler
{
public:
  ObDirectLoadLobIdConflictHandler() {}
  virtual ~ObDirectLoadLobIdConflictHandler() {}
  int handle_insert_row(const blocksstable::ObDatumRow &row) override { return OB_SUCCESS; }
  int handle_update_row(const blocksstable::ObDatumRow &row) override { return OB_ERR_UNEXPECTED; }
  int handle_update_row(common::ObArray<const ObDirectLoadExternalRow *> &rows,
                        const ObDirectLoadExternalRow *&row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_update_row(common::ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
                        const ObDirectLoadMultipleDatumRow *&row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  int handle_update_row(const blocksstable::ObDatumRow &old_row,
                        const blocksstable::ObDatumRow &new_row,
                        const blocksstable::ObDatumRow *&result_row) override
  {
    return OB_ERR_UNEXPECTED;
  }
  TO_STRING_EMPTY();
};

struct ObDirectLoadLobMetaIterParam
{
public:
  ObDirectLoadLobMetaIterParam();
  ~ObDirectLoadLobMetaIterParam();
  bool is_valid() const;

public:
  ObTabletID tablet_id_;
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
};

class ObDirectLoadLobMetaRowIter
{
public:
  ObDirectLoadLobMetaRowIter();
  ~ObDirectLoadLobMetaRowIter();
  int init(const ObDirectLoadLobMetaIterParam &param, ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
           const blocksstable::ObDatumRange &range);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row);

private:
  int init_range();
  int switch_next_lob_id();

private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadLobMetaIterParam param_;
  ObDirectLoadMultipleDatumRange scan_range_;
  ObDirectLoadMultipleSSTableScanMerge scan_merge_;
  ObDirectLoadOriginTable *origin_table_;
  ObIStoreRowIterator *origin_iter_;
  common::ObArenaAllocator range_allocator_;
  blocksstable::ObDatumRange range_;
  ObDirectLoadLobIdConflictHandler conflict_handler_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
