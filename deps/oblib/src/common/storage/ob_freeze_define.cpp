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

#include "common/storage/ob_freeze_define.h"

namespace oceanbase {
namespace storage {
int64_t ObFrozenStatus::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_FROZEN_VERSION, frozen_version_, "frozen_timestamp", frozen_timestamp_, "freeze_status", status_,N_SCHEMA_VERSION, schema_version_, "cluster_version", cluster_version_);
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObFrozenStatus, frozen_version_, frozen_timestamp_, status_, schema_version_, cluster_version_);
}  // end namespace storage
}  // end namespace oceanbase
