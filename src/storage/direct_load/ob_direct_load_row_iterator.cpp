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
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_row_iterator.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;

int ObDirectLoadStoreRowIteratorWrap::init(ObIStoreRowIterator *iter,
                                           const ObDirectLoadRowFlag &row_flag,
                                           const int64_t column_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadStoreRowIteratorWrap init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == iter || column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(iter), K(row_flag), K(column_count));
  } else {
    iter_ = iter;
    row_flag_ = row_flag;
    column_count_ = column_count;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadStoreRowIteratorWrap::get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadStoreRowIteratorWrap not init", KR(ret), KP(this));
  } else {
    ret = iter_->get_next_row(row);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
