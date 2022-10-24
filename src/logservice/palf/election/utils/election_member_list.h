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

#ifndef LOGSERVICE_PALF_ELECTION_UTILS_OB_ELECTION_MEMBERLIST_H
#define LOGSERVICE_PALF_ELECTION_UTILS_OB_ELECTION_MEMBERLIST_H

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "logservice/palf/log_meta_info.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

class MemberList
{
public:
  MemberList();
  bool only_membership_version_different(const MemberList &rhs) const;
  bool operator==(const MemberList &rhs) const;
  bool operator!=(const MemberList &rhs) const;
  int assign(const MemberList &rhs);
  LogConfigVersion get_membership_version() const;
  void set_membership_version(const LogConfigVersion version);
  int64_t get_replica_num() const;
  int set_new_member_list(const common::ObArray<common::ObAddr> &addr_list,
                          const LogConfigVersion membership_version,
                          const int64_t replica_num);
  const common::ObArray<common::ObAddr> &get_addr_list() const;
  bool is_valid() const;
  TO_STRING_KV(K_(addr_list), K_(membership_version), K_(replica_num));
private:
  common::ObArray<common::ObAddr> addr_list_;
  LogConfigVersion membership_version_;
  uint8_t replica_num_;
};

}
}
}

#endif