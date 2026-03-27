/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
