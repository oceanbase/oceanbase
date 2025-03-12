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

#include "ob_memtable_key.h"

namespace oceanbase
{
namespace memtable
{
int ObMemtableKeyGenerator::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init ObMemtableKeyGenerator twice", K(ret));
  } else if (columns_.count() < rowkey_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "rowkey number mismatched", K(columns_.count()), K(rowkey_cnt_));
  } else if (OB_FAIL(obj_buf_.init(&allocator_))) {
    TRANS_LOG(WARN, "init obj buffer fail", K(ret));
  } else if (OB_FAIL(obj_buf_.reserve(rowkey_cnt_))) {
    TRANS_LOG(WARN, "reserve obj buffer fail", K(ret), K(rowkey_cnt_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMemtableKeyGenerator::generate_memtable_key(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMemtableKeyGenerator not inited", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(row));
  } else {
    ObObj *objs = obj_buf_.get_data();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt_; i++) {
      if (OB_FAIL(row.storage_datums_[i].to_obj_enhance(objs[i], columns_.at(i).col_type_))) {
        TRANS_LOG(WARN, "failed to transfer datum to obj", K(ret), K(i), K(row));
      } else if (row.storage_datums_[i].has_lob_header()) {
        objs[i].set_has_lob_header();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(store_rowkey_.assign(objs, rowkey_cnt_))) {
        TRANS_LOG(WARN, "failed to assign rowkey", K(ret), K(row), K(objs), K(rowkey_cnt_));
      } else if (OB_FAIL(memtable_key_.encode(columns_, &store_rowkey_))) {
        TRANS_LOG(WARN, "memtable key encode failed", K(ret), K(row), K(columns_), K(store_rowkey_));
      }
    }
  }
  return ret;
}

} // namespace memtable
} // namespace oceanbase