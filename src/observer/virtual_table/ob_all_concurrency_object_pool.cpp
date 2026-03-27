/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_all_concurrency_object_pool.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::observer;

int ObAllConcurrencyObjectPool::inner_get_next_row(ObNewRow *& row)
{
  UNUSED(row);
  return OB_ITER_END;
}
