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

#ifndef OCEANBASE_UNUTTEST_CLOG_MOCK_OB_PS_CB_H_
#define OCEANBASE_UNUTTEST_CLOG_MOCK_OB_PS_CB_H_

#include "share/ob_i_ps_cb.h"
#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"

namespace oceanbase {
namespace common {
class MockObPSCb : public share::ObIPSCb {
public:
  MockObPSCb()
  {}
  virtual ~MockObPSCb()
  {}

public:
  int64_t get_min_using_file_id() const
  {
    return 0;
  }
  int on_leader_revoke(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return OB_SUCCESS;
  }
  int on_leader_takeover(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return OB_SUCCESS;
  }
  int on_member_change_success(const common::ObPartitionKey& partition_key, const int64_t mc_timestamp,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list)
  {
    UNUSED(partition_key);
    UNUSED(mc_timestamp);
    UNUSED(prev_member_list);
    UNUSED(curr_member_list);
    return OB_SUCCESS;
  }
  int on_leader_active(const oceanbase::common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return OB_SUCCESS;
  }
  bool is_take_over_done(const oceanbase::common::ObPartitionKey& pkey) const
  {
    UNUSED(pkey);
    return true;
  }
  bool is_revoke_done(const oceanbase::common::ObPartitionKey& pkey) const
  {
    UNUSED(pkey);
    return true;
  }
  virtual bool is_tenant_active(const common::ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return true;
  }
  virtual int get_leader_from_loc_cache(const common::ObPartitionKey& pkey, ObAddr& leader, const bool is_need_renew)
  {
    UNUSED(pkey);
    UNUSED(leader);
    UNUSED(is_need_renew);
    return common::OB_SUCCESS;
  }
  virtual int handle_log_missing(const common::ObPartitionKey& pkey, const common::ObAddr& server)
  {
    UNUSED(pkey);
    UNUSED(server);
    return common::OB_SUCCESS;
  }
  virtual int get_server_locality_array(
      common::ObIArray<share::ObServerLocality>& server_locality_array, bool& has_readonly_zone) const
  {
    UNUSED(server_locality_array);
    UNUSED(has_readonly_zone);
    return common::OB_SUCCESS;
  }
  virtual int check_partition_index_available(const common::ObPartitionKey& pkey, bool& available)
  {
    UNUSED(pkey);
    UNUSED(available);
    return common::OB_SUCCESS;
  }
  virtual int get_last_ssstore_version(
      const oceanbase::common::ObPartitionKey& pkey, oceanbase::common::ObVersion& version)
  {
    UNUSED(pkey);
    UNUSED(version);
    return common::OB_SUCCESS;
  }
};
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_UNUTTEST_CLOG_MOCK_OB_PS_CB_H_
