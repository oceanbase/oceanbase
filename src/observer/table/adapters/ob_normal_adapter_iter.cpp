/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
 