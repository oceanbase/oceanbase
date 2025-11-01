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
#include "storage/ddl/ob_tablet_slice_row_iterator.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "sql/das/ob_das_utils.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_lob_macro_block_writer.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::sql;

ObTabletSliceRowIterator::ObTabletSliceRowIterator()
  : is_inited_(false), arena_("slice_row_iter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), tablet_id_(), slice_idx_(-1), row_iter_(nullptr),
    lob_writer_(nullptr)
{

}

ObTabletSliceRowIterator::~ObTabletSliceRowIterator()
{
  if (nullptr != lob_writer_) {
    lob_writer_->~ObLobMacroBlockWriter();
    ob_free(lob_writer_);
    lob_writer_ = nullptr;
  }
}

int ObTabletSliceRowIterator::init(const ObTabletID &tablet_id,
                                   const int64_t slice_idx,
                                   const ObWriteMacroParam &write_param,
                                   ObIStoreRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(
        !tablet_id.is_valid()
        || slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(slice_idx));
  } else {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    row_iter_ = &row_iter;
    param_ = &write_param;
    is_inited_ = true;
    LOG_INFO("tablet slice row iter init finished", KPC(this));
  }
  return ret;
}

int ObTabletSliceRowIterator::get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  arena_.reuse();
  const blocksstable::ObDatumRow *current_row = nullptr;
  // get next row
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  // convert sql row to storage row
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter_->get_next_row(current_row))) {
      LOG_WARN("eval current row failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::convert_to_storage_row(tablet_id_, slice_idx_, *param_, lob_writer_, arena_, const_cast<blocksstable::ObDatumRow &>(*current_row)))) {
      LOG_WARN("convert sql row to storage row failed", K(ret));
    }
  }

  if (OB_ITER_END == ret && nullptr != lob_writer_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(lob_writer_->close())) {
      LOG_WARN("lob writer close failed", K(tmp_ret));
      ret = tmp_ret;
    }
  }
  if (OB_SUCC(ret)) {
    row = current_row;
  }

  LOG_TRACE("tablet slice row iter get next row", K(ret), KPC(row));
  return ret;
}

int ObTabletSliceRowIterator::get_next_batch(const ObBatchDatumRows *&datum_rows)
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}
