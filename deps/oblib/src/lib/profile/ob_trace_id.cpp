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
