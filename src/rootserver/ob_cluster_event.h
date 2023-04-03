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

#ifndef OCEANBASE_ROOTSERVICE_OB_CLUSTER_EVENT_H__
#define OCEANBASE_ROOTSERVICE_OB_CLUSTER_EVENT_H__

#include "lib/utility/ob_print_kv.h"
#include "lib/profile/ob_trace_id.h"
// __all_rootservice_event_history 'value' column length is only 256 which is not enough to hold
// cluster event info. So use 'extra_info' column instead whose length is 512.
#define CLUSTER_EVENT_ADD(level, error_code, event, args...) \
    do { \
      const int64_t MAX_VALUE_LENGTH = 512; \
      char VALUE[MAX_VALUE_LENGTH]; \
      int64_t pos = 0; \
      ::oceanbase::common::ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();\
      ::oceanbase::common::databuff_print_kv(VALUE, MAX_VALUE_LENGTH, pos, ##args, KPC(trace_id)); \
      ROOTSERVICE_EVENT_ADD("cluster_event", event, "event_type", #level, "ret", error_code, NULL, "", NULL, "", NULL, "", "message", "", ObHexEscapeSqlStr(VALUE)); \
    } while (0)

#define CLUSTER_EVENT_ADD_CONTROL(error_code, event, args...) \
    CLUSTER_EVENT_ADD(CONTROL, error_code, event, ##args)

#define CLUSTER_EVENT_ADD_CONTROL_START(error_code, event, args...) \
    CLUSTER_EVENT_ADD(CONTROL, error_code, event, "flag", "start", ##args)

#define CLUSTER_EVENT_ADD_CONTROL_FINISH(error_code, event, args...) \
    CLUSTER_EVENT_ADD(CONTROL, error_code, event,  "flag", "finish", ##args)

#define CLUSTER_EVENT_ADD_LOG(error_code, event, args...)\
    do {\
      if (OB_SUCCESS == error_code) {\
        CLUSTER_EVENT_ADD(INFO, error_code, event, ##args);\
      } else {\
        CLUSTER_EVENT_ADD(WARN, error_code, event, ##args);\
      }\
    } while (0)

#endif
