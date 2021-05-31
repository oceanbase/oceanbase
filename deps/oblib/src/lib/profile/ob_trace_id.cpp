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

#include "lib/atomic/ob_atomic.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_serialization_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase {
namespace common {
uint64_t ObCurTraceId::SeqGenerator::seq_generator_ = 0;

OB_SERIALIZE_MEMBER(ObCurTraceId::TraceId, uval_[0], uval_[1]);
int32_t LogExtraHeaderCallback(
    char* buf, int32_t buf_size, int level, const char* file, int line, const char* function, pthread_t tid)
{
  int32_t ret = 0;
  UNUSED(level);
  UNUSED(file);
  UNUSED(line);
  UNUSED(function);
  UNUSED(tid);
  const uint64_t* tval = ObCurTraceId::get();
  if (OB_ISNULL(tval)) {
    fprintf(stderr, "fail to get trace id\n");
  } else {
    ret = snprintf(buf, buf_size, "[%ld][" TRACE_ID_FORMAT "] ", GETTID(), tval[0], tval[1]);
    if (ret >= buf_size) {
      ret = buf_size - 1;
    }
  }
  return ret;
}

ObCurTraceId::Guard::Guard(const ObAddr& addr) : need_reset_(false)
{
  const uint64_t* trace_id = ObCurTraceId::get();
  if (0 == trace_id[0]) {
    ObCurTraceId::init(addr);
    need_reset_ = true;
  }
}

ObCurTraceId::Guard::~Guard()
{
  if (need_reset_) {
    ObCurTraceId::reset();
  }
}

}  // end namespace common
}  // end namespace oceanbase
