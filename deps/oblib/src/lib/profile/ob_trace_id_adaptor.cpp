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

#include "lib/profile/ob_trace_id_adaptor.h"
#include "lib/profile/ob_trace_id.h"

using namespace oceanbase::common;

OB_SERIALIZE_MEMBER(ObTraceIdAdaptor, uval_[0], uval_[1], uval_[2], uval_[3]);

int64_t ObTraceIdAdaptor::to_string(char *buf, const int64_t buf_len) const
{
  ObCurTraceId::TraceId trace_id;
  trace_id.set(uval_);
  return trace_id.to_string(buf, buf_len);
}
