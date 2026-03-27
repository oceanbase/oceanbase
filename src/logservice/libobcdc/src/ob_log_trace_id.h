/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * OBCDC general tools
 */

#ifndef OCEANBASE_LIBOBCDC_SRC_OB_LOG_TRACE_ID_
#define OCEANBASE_LIBOBCDC_SRC_OB_LOG_TRACE_ID_

#include "lib/net/ob_addr.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase
{
namespace libobcdc
{
common::ObAddr& get_self_addr();

// trace id: Used to identify rpc requests between libobcdc-observer
inline void init_trace_id()
{
  common::ObCurTraceId::init(get_self_addr());
}

inline void clear_trace_id()
{
  common::ObCurTraceId::reset();
}

inline void set_trace_id(const common::ObCurTraceId::TraceId &trace_id)
{
  common::ObCurTraceId::set(trace_id);
}

class ObLogTraceIdGuard
{
public:
  ObLogTraceIdGuard()
  {
    init_trace_id();
  }

  explicit ObLogTraceIdGuard(const common::ObCurTraceId::TraceId &trace_id)
  {
    set_trace_id(trace_id);
  }

  ~ObLogTraceIdGuard()
  {
    clear_trace_id();
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTraceIdGuard);
};

}
}
#endif // OCEANBASE_LIBOBCDC_SRC_OB_LOG_TRACE_ID_
