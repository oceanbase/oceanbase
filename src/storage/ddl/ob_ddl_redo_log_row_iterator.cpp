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

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_ddl_redo_log_row_iterator.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace transaction;
using namespace share::schema;

ObDDLRedoLogRowIterator::ObDDLRedoLogRowIterator(ObIAllocator &allocator, const uint64_t tenant_id)
  : allocator_(allocator),
    iter_(allocator, tenant_id),
    rowkey_obobj_(nullptr),
    schema_rowkey_column_count_(0),
    is_inited_(false)
{
}

ObDDLRedoLogRowIterator::~ObDDLRedoLogRowIterator()
{
  reset();
}

int ObDDLRedoLogRowIterator::init(const ObString &data_buffer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter_.open(data_buffer.ptr(), data_buffer.length()))) {
    LOG_WARN("fail to open iter_", KR(ret), K(data_buffer));
  } else if (FALSE_IT(schema_rowkey_column_count_ = iter_.get_rowkey_column_descs().count() - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt())) {
  } else if (OB_UNLIKELY(schema_rowkey_column_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_rowkey_column_count_ invalid", KR(ret), K(schema_rowkey_column_count_));
  } else if (OB_ISNULL(rowkey_obobj_ = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * schema_rowkey_column_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObDDLRedoLogRowIterator::reset()
{
  iter_.reset();
  rowkey_.reset();
  if (rowkey_obobj_ != nullptr) {
    allocator_.free(rowkey_obobj_);
    rowkey_obobj_ = nullptr;
  }
  schema_rowkey_column_count_ = 0;
  is_inited_ = false;
}

int ObDDLRedoLogRowIterator::get_next_row(const ObDatumRow *&row,
                                             const ObStoreRowkey *&rowkey,
                                             ObTxSEQ &seq_no,
                                             blocksstable::ObDmlRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  rowkey = nullptr;
  seq_no.reset();
  row_flag.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(iter_.get_next_row(row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", KR(ret));
    }
  } else {
    const ObStorageDatum *datums = row->storage_datums_;
    const ObIArray<ObColDesc> &col_desc = iter_.get_rowkey_column_descs();
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_rowkey_column_count_; i++) {
      if (OB_FAIL(datums[i].to_obj_enhance(rowkey_obobj_[i], col_desc.at(i).col_type_))) {
        LOG_WARN("fail to get obobj", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowkey_.assign(rowkey_obobj_, schema_rowkey_column_count_))) {
        LOG_WARN("fail to assign rowkey", KR(ret));
      } else {
        rowkey = &rowkey_;
        seq_no = ObTxSEQ::cast_from_int(-datums[schema_rowkey_column_count_ + 1].get_int());
        row_flag = row->row_flag_;
      }
    }
  }

  return ret;
}

int ObDDLRedoLogRowIterator::get_next_lob_meta_row(const ObLobMetaInfo *&row,
                                                      const ObStoreRowkey *&rowkey,
                                                      ObTxSEQ &seq_no,
                                                      blocksstable::ObDmlRowFlag &row_flag)
{
  // 一期暂不支持
  return OB_NOT_SUPPORTED;
}

} // namespace storage
} // namespace oceanbase
