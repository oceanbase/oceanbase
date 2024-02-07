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

#include "share/stat/ob_opt_osg_column_stat.h"
#include "storage/direct_load/ob_direct_load_lob_builder.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace blocksstable
{
class ObIStoreRowIterator;
} // namespace blocksstable
namespace storage
{
struct ObDirectLoadInsertTableRowIteratorParam
{
public:
  ObDirectLoadInsertTableRowIteratorParam();
  ~ObDirectLoadInsertTableRowIteratorParam();
  TO_STRING_KV(K_(tablet_id), K_(table_data_desc), K_(is_heap_table), K_(online_opt_stat_gather), K_(px_mode));
public:
  ObTabletID tablet_id_;
  ObDirectLoadTableDataDesc table_data_desc_;
  int64_t lob_column_cnt_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStoreCmpFuncs *cmp_funcs_;
  common::ObIArray<common::ObOptOSGColumnStat *> *column_stat_array_;
  ObDirectLoadLobBuilder *lob_builder_;
  bool is_heap_table_;
  bool online_opt_stat_gather_;
  bool px_mode_;
};

class ObDirectLoadInsertTableRowIterator : public ObIStoreRowIterator
{
public:
  ObDirectLoadInsertTableRowIterator();
  virtual ~ObDirectLoadInsertTableRowIterator();
  int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
private:
  int collect_obj(const blocksstable::ObDatumRow &datum_row);
  int handle_lob(blocksstable::ObDatumRow &datum_row);
protected:
  int inner_init(const ObDirectLoadInsertTableRowIteratorParam &param);
  virtual int inner_get_next_row(blocksstable::ObDatumRow *&datum_row) = 0;
private:
  ObDirectLoadInsertTableRowIteratorParam param_;
  common::ObArenaAllocator lob_allocator_;
};

} // namespace storage
} // namespace oceanbase
