/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define private public
#define protected public
#include "share/compaction/ob_compaction_locality_cache.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{

TEST(ObCompactionLocalityCache, skip_ls_without_replica_after_filter)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const share::ObLSID ls_id(share::ObLSID::SYS_LS_ID);
  share::ObCompactionLocalityCache cache;
  share::ObLSInfo source_ls_info(tenant_id, ls_id);
  share::ObLSReplica replica;
  share::ObLSReplica::MemberList member_list;
  common::GlobalLearnerList learner_list;
  common::ObAddr server;
  common::ObSEArray<common::ObZone, 1> zone_list;

  ASSERT_EQ(OB_SUCCESS, cache.init(tenant_id));
  ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2882));
  ASSERT_EQ(OB_SUCCESS, member_list.push_back(share::SimpleMember(server, 1)));
  ASSERT_EQ(OB_SUCCESS,
      replica.init(
          1 /*create_time_us*/,
          1 /*modify_time_us*/,
          tenant_id,
          ls_id,
          server,
          2881 /*sql_port*/,
          common::LEADER,
          common::REPLICA_TYPE_FULL,
          1 /*proposal_id*/,
          share::REPLICA_STATUS_NORMAL,
          share::ObLSRestoreStatus(),
          100 /*memstore_percent*/,
          1 /*unit_id*/,
          common::ObString::make_string("zone1"),
          1 /*paxos_replica_number*/,
          0 /*data_size*/,
          0 /*required_size*/,
          member_list,
          learner_list,
          false /*rebuild*/));
  ASSERT_EQ(OB_SUCCESS, source_ls_info.add_replica(replica));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(common::ObZone("zone2")));

  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      cache.refresh_by_zone(source_ls_info, zone_list));

  share::ObLSInfo result;
  ASSERT_TRUE(cache.empty());
  ASSERT_EQ(OB_HASH_NOT_EXIST, cache.get_ls_info(ls_id, result));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
