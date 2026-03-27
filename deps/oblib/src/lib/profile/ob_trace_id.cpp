/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/profile/ob_trace_id.h"
using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{
#ifdef COMPILE_DLL_MODE
TLOCAL(ObCurTraceId::TraceId, ObCurTraceId::trace_id_);
#endif

uint64_t ObCurTraceId::SeqGenerator::seq_generator_ = 0;

OB_SERIALIZE_MEMBER(ObCurTraceId::TraceId, uval_[0], uval_[1], uval_[2], uval_[3]);

} // end namespace common
} // end namespace oceanbase

extern "C" {
  const char* trace_id_to_str_c(const uint64_t *uval, char *buf, int64_t buf_len)
  {
    if (nullptr != buf && buf_len > 0) {
      ObCurTraceId::TraceId trace_id;
      trace_id.set(uval);
      int64_t pos = 0;
      (void)databuff_printf(buf, buf_len, pos, trace_id);
    }
    return buf;
  }
} /* extern "C" */
