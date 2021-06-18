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

#define USING_LOG_PREFIX SQL_DTL

#include "ob_op_metric.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER(ObOpMetric, enable_audit_, id_, type_, first_in_ts_, first_out_ts_, last_in_ts_, last_out_ts_,
    counter_, exec_time_);
int64_t ObOpMetric::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(id), K_(type), K_(first_in_ts), K_(first_out_ts), K_(last_in_ts), K_(last_out_ts), K_(counter), K_(exec_time));
  J_OBJ_END();
  return pos;
}
