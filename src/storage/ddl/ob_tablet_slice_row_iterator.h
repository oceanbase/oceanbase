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

#ifndef _STORAGE_DDL_OB_TABLET_SLICE_ROW_ITERATOR_
#define _STORAGE_DDL_OB_TABLET_SLICE_ROW_ITERATOR_

#include "storage/ddl/ob_ddl_struct.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"

namespace oceanbase
{

namespace storage
{
class ObDDLIndependentDag;
class ObLobMacroBlockWriter;
class ObWriteMacroParam;

class ObITabletSliceRowIterator
{
public:
  ObITabletSliceRowIterator() = default;
  virtual ~ObITabletSliceRowIterator() = default;
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) = 0;
  virtual int get_next_batch(const blocksstable::ObBatchDatumRows *&datum_rows) = 0;
  virtual int64_t get_slice_idx() const = 0;
  virtual ObTabletID get_tablet_id() const = 0;
  virtual int64_t get_max_batch_size() const { return OB_NOT_IMPLEMENT;};
};

class ObTabletSliceRowIterator : public ObITabletSliceRowIterator
{
public:
  ObTabletSliceRowIterator();
  virtual ~ObTabletSliceRowIterator();
  int init(const ObTabletID &tablet_id,
           const int64_t slice_idx,
           const ObWriteMacroParam &param,
           ObIStoreRowIterator &row_iter);
  int get_next_row(const blocksstable::ObDatumRow *&row) override;
  int get_next_batch(const blocksstable::ObBatchDatumRows *&datum_rows) override;
  int64_t get_slice_idx() const override { return slice_idx_; }
  ObTabletID get_tablet_id() const override { return tablet_id_; }

  VIRTUAL_TO_STRING_KV(K(is_inited_), K(tablet_id_), K(slice_idx_), KP(lob_writer_));

private:
  bool is_inited_;
  ObArenaAllocator arena_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;

  ObIStoreRowIterator *row_iter_;
  const ObWriteMacroParam *param_;

  // lob info
  ObLobMacroBlockWriter *lob_writer_;
};

class ObITabletSliceRowIterIterator
{
public:
  ObITabletSliceRowIterIterator() = default;
  virtual ~ObITabletSliceRowIterIterator() = default;
  virtual int get_next_iter(ObITabletSliceRowIterator *&iter) = 0;
};

}// namespace storage
}// namespace oceanbase

#endif//_STORAGE_DDL_OB_TABLET_SLICE_ROW_ITERATOR_
