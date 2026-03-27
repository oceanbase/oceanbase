/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_multi_batch_common.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;

bool ObTableMultiBatchRequest::is_valid() const
{
  return !ops_.empty() && !tablet_ids_.empty() && (ops_.count() == tablet_ids_.count());
}
