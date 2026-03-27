/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_GTS_DEFINE_
#define OCEANBASE_TRANSACTION_OB_GTS_DEFINE_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "share/ob_define.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "ob_trans_define.h"

namespace oceanbase
{
namespace transaction
{

typedef common::ObIntWarp ObTsTenantInfo;

enum ObGTSCacheTaskType
{
  INVALID_GTS_TASK_TYPE = -1,
  GET_GTS = 0,
  WAIT_GTS_ELAPSING,
};

inline bool atomic_update(int64_t *v, const int64_t x)
{
  bool bool_ret = false;
  int64_t ov = ATOMIC_LOAD(v);
  while (ov < x) {
    if (ATOMIC_BCAS(v, ov, x)) {
      bool_ret = true;
      break;
    } else {
      ov = ATOMIC_LOAD(v);
    }
  }
  return bool_ret;
}

} // transaction
} // oceanbase

#endif // OCEANBASE_RANSACTION_OB_GTS_DEFINE_
