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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_px_op_size_factor.h"

using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER(PxOpSizeFactor, factor_);
int64_t PxOpSizeFactor::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(block_granule_child), K_(block_granule_parent), K_(partition_granule_child),K_(partition_granule_parent), K_(single_partition_table_scan), K_(broadcast_exchange), K_(pk_exchange));
  J_OBJ_END();
  return pos;
}
