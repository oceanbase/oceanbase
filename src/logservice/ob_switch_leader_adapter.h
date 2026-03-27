/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LEADER_SWITCH_
#define OCEANBASE_LOGSERVICE_OB_LEADER_SWITCH_

#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace logservice
{

class ObSwitchLeaderAdapter
{
public:
  ObSwitchLeaderAdapter() { }
  ~ObSwitchLeaderAdapter() { }

  static int add_to_election_blacklist(const int64_t palf_id, const common::ObAddr &server);
  static int remove_from_election_blacklist(const int64_t palf_id, const common::ObAddr &server);
  static int set_election_blacklist(const int64_t palf_id, const common::ObAddr &server);

private:
  static int is_meta_tenant_dropped_(const uint64_t tenant_id, bool &is_dropped);
};
} // end namespace logservice
} // end namespace oceanbase
#endif