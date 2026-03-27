/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
