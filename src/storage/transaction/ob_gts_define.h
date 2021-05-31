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
#include "common/ob_partition_key.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "ob_trans_define.h"

namespace oceanbase {
namespace transaction {

typedef common::ObIntWarp ObTsTenantInfo;

enum ObGTSCacheTaskType {
  INVALID_GTS_TASK_TYPE = -1,
  GET_GTS = 0,
  WAIT_GTS_ELAPSING,
};

inline int get_gts_pkey(const uint64_t tenant_id, common::ObPartitionKey& pkey)
{
  int ret = common::OB_SUCCESS;
  if (!common::is_valid_tenant_id(tenant_id)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    const uint64_t pure_id = ((common::OB_SYS_TENANT_ID == tenant_id) ? oceanbase::share::OB_ALL_CORE_TABLE_TID
                                                                      : oceanbase::share::OB_ALL_DUMMY_TID);
    const uint64_t table_id = common::combine_id(tenant_id, pure_id);
    const int64_t part_cnt = ((common::OB_SYS_TENANT_ID == tenant_id) ? 1 : 0);
    pkey = common::ObPartitionKey(table_id, 0, part_cnt);
  }
  return ret;
}

inline uint64_t get_gts_table_id(const uint64_t tenant_id)
{
  const uint64_t pure_id = ((common::OB_SYS_TENANT_ID == tenant_id) ? oceanbase::share::OB_ALL_CORE_TABLE_TID
                                                                    : oceanbase::share::OB_ALL_DUMMY_TID);
  return common::combine_id(tenant_id, pure_id);
}

inline int64_t get_gts_partition_id()
{
  return static_cast<int64_t>(0);
}

inline bool atomic_update(int64_t* v, const int64_t x)
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

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_RANSACTION_OB_GTS_DEFINE_
