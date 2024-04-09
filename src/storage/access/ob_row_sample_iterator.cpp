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

#include "ob_row_sample_iterator.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
int ObMemtableRowSampleIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iterator_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "row sample iterator is not inited", K(ret), KP_(iterator));
  } else if (OB_FAIL(iterator_->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "multiple merge failed to get next row", K(ret));
    } else {
      STORAGE_LOG(INFO, "total sample row count", K(row_num_));
    }
  } else {
    row_num_++;
  }
  return ret;
}
}
}
