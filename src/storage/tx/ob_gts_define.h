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
