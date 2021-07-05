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

#ifndef OCEANBASE_COMMON_OB_TRACE_PROFILE_H_
#define OCEANBASE_COMMON_OB_TRACE_PROFILE_H_

#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace common {
class ObTraceProfile {
public:
  ObTraceProfile();
  ~ObTraceProfile()
  {}

public:
  int init(const char* module, const int64_t warn_timeout, const bool is_tracing);
  void set_sign(const uint64_t sign);
  void reset();
  int trace(const ObPartitionKey& partition_key, const char* flag);
  int trace(const ObPartitionKey& partition_key, const char* flag, const int64_t time);
  int trace(const ObPartitionKey& partition_key, const ObAddr& server);
  int trace(const ObPartitionKey& partition_key, const char* flag, const ObAddr& server);
  void report_trace();

private:
  struct TraceEntry {
    void reset();
    ObPartitionKey partition_key_;
    ObAddr server_;
    const char* flag_;
    int64_t time_;
  };

  static const int64_t MAX_TRACE_NUM = 16;
  static const int64_t MAX_OUTPUT_BUFFER = 4096;

  const char* module_name_;
  uint64_t sign_;
  int64_t warn_timeout_;
  int32_t idx_;
  bool is_tracing_;
  bool is_inited_;

  TraceEntry entry_[MAX_TRACE_NUM];

private:
  DISALLOW_COPY_AND_ASSIGN(ObTraceProfile);
};

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_TRACE_PROFILE_H_
