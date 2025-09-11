/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_dummy_table_row_iter.h"

namespace oceanbase
{
namespace sql {


int ObDummyTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  return OB_SUCCESS;
}

int ObDummyTableRowIterator::get_next_row()
{
  return OB_ITER_END;
}

int ObDummyTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  return OB_ITER_END;
}

void ObDummyTableRowIterator::reset()
{
  // do nothing
}

}
}
