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
 *
 * OBCDC general tools
 */

#ifndef OCEANBASE_LOG_FETCHER_SRC_OB_LOG_TRACE_ID_
#define OCEANBASE_LOG_FETCHER_SRC_OB_LOG_TRACE_ID_

#include "lib/net/ob_addr.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase
{
namespace logfetcher
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
#endif // OCEANBASE_LOG_FETCHER_SRC_OB_LOG_TRACE_ID_
