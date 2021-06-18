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

#include "share/ob_define.h"
#include "sql/executor/ob_slice_id.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
using namespace common;

namespace sql {
int64_t ObSliceID::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_OB_TASK_ID, ob_task_id_, N_SLICE_ID, slice_id_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObSliceID, ob_task_id_, slice_id_);

}  // namespace sql
}  // namespace oceanbase
