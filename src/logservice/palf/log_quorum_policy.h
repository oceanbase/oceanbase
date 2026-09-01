/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_QUORUM_POLICY_
#define OCEANBASE_LOGSERVICE_LOG_QUORUM_POLICY_

#include <stdint.h>

namespace oceanbase
{
namespace palf
{

class LogQuorumPolicy final
{
public:
  LogQuorumPolicy() = default;
  ~LogQuorumPolicy() = default;

  // Returns the accept quorum (Q2) for the given number of Full replicas participating in log sync.
  int64_t get_accept_quorum(const int64_t replica_num) const;
  // Returns the prepare quorum (Q1) for the given number of Full replicas participating in log sync.
  int64_t get_prepare_quorum(const int64_t replica_num) const;
};

} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_QUORUM_POLICY_
