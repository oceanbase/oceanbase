/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "log_quorum_policy.h"

namespace oceanbase
{
namespace palf
{

int64_t LogQuorumPolicy::get_accept_quorum(const int64_t replica_num) const
{
  return replica_num / 2 + 1;
}

int64_t LogQuorumPolicy::get_prepare_quorum(const int64_t replica_num) const
{
  return replica_num / 2 + 1;
}

} // namespace palf
} // namespace oceanbase
