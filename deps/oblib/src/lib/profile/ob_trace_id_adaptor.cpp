/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
