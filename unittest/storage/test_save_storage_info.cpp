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

#include "clog/ob_log_define.h"
#include "storage/ob_saved_storage_info.h"

#include <gtest/gtest.h>

namespace oceanbase {
using namespace common;
using namespace storage;
namespace unittest {
TEST(ob_save_storage_info_test, test1)
{
  const int64_t BUF_LEN = 1000;
  const int64_t member_number = 3;
  const int32_t begin_port = 3000;
  const int64_t epoch_id = 3333;
  ObProposalID proposal_id;
  proposal_id.addr_.set_ip_addr("127.0.0.1", 8001);
  proposal_id.ts_ = 1;
  const uint64_t last_replay_log_id = 5555;
  const uint64_t last_submit_timestamp = 5555;
  const uint64_t accumulate_checksum = 5555;
  const int64_t replica_num = 1;

  ObAddr server;
  ObMemberList ob_member_list;
  ObAddr addr;
  addr.set_ip_addr("127.0.0.1", 8001);
  ob_member_list.add_server(addr);
  ObBaseStorageInfo base_info;
  ObSavedStorageInfo save_info;
  ObVersion version;
  ObProposalID fake_ms_proposal_id;

  ob_member_list.reset();
  for (int64_t i = 0; i < member_number; ++i) {
    server.reset();
    server.set_ip_addr("127.0.0.1", begin_port + static_cast<int32_t>(i));
    EXPECT_EQ(OB_SUCCESS, ob_member_list.add_member(ObMember(server, OB_INVALID_TIMESTAMP)));
  }

  EXPECT_EQ(OB_SUCCESS,
      base_info.init(epoch_id,
          proposal_id,
          last_replay_log_id,
          last_submit_timestamp,
          accumulate_checksum,
          replica_num,
          0,
          0,
          ob_member_list,
          fake_ms_proposal_id));
  EXPECT_EQ(OB_SUCCESS, save_info.deep_copy(base_info));
  version.version_ = 1;
  save_info.set_memstore_version(version);

  char buf[BUF_LEN];
  int64_t pos = 0;
  ObSavedStorageInfo info;

  EXPECT_EQ(OB_SUCCESS, save_info.serialize(buf, BUF_LEN, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, info.deserialize(buf, BUF_LEN, pos));

  STORAGE_LOG(INFO, "before and after", K(save_info), K(info));
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_saved_storage_info.log", true);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest:test_saved_storage_info");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
