/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "ob_normal_adapter_iter.h"

namespace oceanbase
{
namespace table
{

int ObHbaseNormalCellIter::get_next_cell(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator is not opened", K(ret));
  } else if (OB_FAIL(tb_row_iter_.get_next_row(row))) {
    if (ret == OB_ITER_END) {
      LOG_DEBUG("iterator is end", K(ret));
    } else{
      LOG_WARN("fail to get next cell", K(ret));
    }
  }
  return ret;
}

} // end of namespace table
} // end of namespace oceanbase
