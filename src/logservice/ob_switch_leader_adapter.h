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