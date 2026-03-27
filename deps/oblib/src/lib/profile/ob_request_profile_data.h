/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#ifndef OCEANBASE_COMMON_OB_REQUEST_PROFILE_DATA_H
#define OCEANBASE_COMMON_OB_REQUEST_PROFILE_DATA_H
#define PROFILE_ITEM(target) \
  int64_t target##_start_; \
  int64_t target##_end_;

namespace oceanbase
{
namespace common
{
//POD type
struct ObRequestProfileData
{
  struct ObRpcLatency
  {
    int64_t channel_id_;
    int64_t rpc_start_;
    int64_t rpc_end_;
    int64_t pcode_;
  };
  uint64_t trace_id_[2];
  PROFILE_ITEM(sql_to_logicalplan);
  PROFILE_ITEM(logicalplan_to_physicalplan);
  PROFILE_ITEM(handle_sql_time);
  PROFILE_ITEM(handle_request_time);
  //More special, the start time and end time are handled by two threads, and there can be no thread private
  int64_t wait_sql_queue_time_;
  ObRpcLatency rpc_latency_arr_[256];
  char sql_[512];
  int32_t sql_len_;
  uint8_t count_;
  int64_t pcode_;
  int sql_queue_size_;
};
}
}
#endif // OCEANBASE_COMMON_OB_REQUEST_PROFILE_DATA_H
