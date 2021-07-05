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

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "storage/ob_partition_migrator.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgReferee;

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace transaction;
using namespace share::schema;
using namespace obrpc;
using namespace observer;
using namespace lib;

namespace unittest {

class ObPartitionMigratorTest : public ::testing::Test {
public:
  void init()
  {
    ASSERT_EQ(OB_SUCCESS, a_.parse_from_cstring("10.10.10.1:1001"));
    ASSERT_EQ(OB_SUCCESS, b_.parse_from_cstring("10.10.10.2:1001"));
    ASSERT_EQ(OB_SUCCESS, c_.parse_from_cstring("10.10.10.3:1001"));
  }

  void make_info(
      const int64_t version, const ObAddr& server, uint64_t last_replay_log_id, ObMigrateStoreInfo& fake_info)
  {
    common::ObMemberList member_list;
    const int64_t last_submit_timestamp = 0;
    const int64_t replica_num = 1;

    member_list.add_server(server);
    fake_info.store_info_.sstable_count_ = 2;
    ASSERT_EQ(
        OB_SUCCESS, fake_info.store_info_.saved_storage_info_.init(replica_num, member_list, last_submit_timestamp));
    fake_info.store_info_.saved_storage_info_.frozen_version_ = version;
    fake_info.store_info_.saved_storage_info_.memstore_version_ = version + 1;
    fake_info.store_info_.saved_storage_info_.last_replay_log_id_ = last_replay_log_id;
    fake_info.server_ = server;
  }

  int check_info(ObMigrateInfoFetchResult output, const int64_t index, const int64_t major, const ObAddr& server,
      const int64_t minor = -1)
  {
    int ret = OB_SUCCESS;

    if (server != output.info_list_[index].server_) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "server not match", K(server), K(index), K(output));
    }
    if (major != output.info_list_[index].get_version().major_) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "version not match", K(major), K(index), K(output));
    }
    if (minor >= 0 && minor != output.info_list_[index].get_version().minor_) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "minor version not match", K(minor), K(index), K(output));
    }
    return ret;
  }

protected:
  ObAddr a_;
  ObAddr b_;
  ObAddr c_;
};

TEST_F(ObPartitionMigratorTest, update_base_source_use_candidate)
{
  init();
  ObMigrateInfoFetchResult candidate;
  ObMigrateInfoFetchResult output;
  ObMigrateStoreInfo fake_info;
  make_info(2, a_, 100, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 0, ObVersion(0), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 2, a_));
}

TEST_F(ObPartitionMigratorTest, update_base_source_invalid_candidate)
{
  init();
  ObMigrateInfoFetchResult candidate;
  ObMigrateInfoFetchResult output;
  ObMigrateStoreInfo fake_info;
  make_info(2, a_, 100, fake_info);
  fake_info.store_info_.saved_storage_info_.memstore_version_ = 2;
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_ERR_SYS, ObPartitionMigrateTask::update_base_source(candidate, 0, ObVersion(0), output));
}

TEST_F(ObPartitionMigratorTest, update_base_source_candidate_not_continues)
{
  init();
  ObMigrateInfoFetchResult candidate;
  ObMigrateInfoFetchResult output;
  ObMigrateStoreInfo fake_info;
  make_info(2, a_, 100, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_ERR_SYS, ObPartitionMigrateTask::update_base_source(candidate, 0, ObVersion(0), output));
}

TEST_F(ObPartitionMigratorTest, update_base_source_output_not_continues)
{
  init();
  ObMigrateInfoFetchResult candidate;
  ObMigrateInfoFetchResult output;
  ObMigrateStoreInfo fake_info;
  make_info(2, a_, 100, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(2, a_, 100, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_ERR_SYS, ObPartitionMigrateTask::update_base_source(candidate, 400, ObVersion(5), output));
}

// output  candidate     new_output
// 3-a     3-b           3-a
// 3-a,4-a 3-b,4-b       3-a,4-a
// 3-a,4-a 3-b,4-b,5-b   3-a,4-a,5-b
// 3-a,4-b 3-c,4-c,5-c   3-a,4-b,5-c
// 3-a,4-a 3-b,4-b,5-b,6-b   3-a,4-a,5-b
TEST_F(ObPartitionMigratorTest, update_base_source_same_base_version)
{
  init();
  ObMigrateInfoFetchResult candidate;
  ObMigrateInfoFetchResult output;
  ObMigrateStoreInfo fake_info;

  // 3-a     3-b           3-a
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(3, b_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 500, ObVersion(6), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));

  // 3-a,4-a 3-b,4-b       3-a,4-a
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 500, ObVersion(6), output));
  ASSERT_EQ(2, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, a_));

  // 3-a,4-a 3-b,4-b,5-b   3-a,4-a,5-b
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 500, ObVersion(6), output));
  ASSERT_EQ(3, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 2, 5, b_));

  // 3-a,4-b 3-c,4-c,5-c   3-a,4-b,5-c
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(3, c_, 200, fake_info);
  candidate.info_list_.reset();
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(4, c_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(5, c_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 500, ObVersion(6), output));
  ASSERT_EQ(3, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, b_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 2, 5, c_));

  // 3-a,4-a 3-b,4-b,5-b,6-b   3-a,4-a,5-b
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(3, b_, 200, fake_info);
  candidate.info_list_.reset();
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(6, b_, 500, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 500, ObVersion(6), output));
  ASSERT_EQ(3, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 2, 5, b_));
}

// output      candidate         new_output
// 3-a         2-b                 3-a
// 3-a,4-a     2-b,3-b           3-a,4-a
// 3-a,        2-b,3-b,4-b       3-a,4-b
// 3-a,4-b     2-c,3-c,4-c,5-c   3-a,4-b,5-c
// 5-a          3-b,4-b            5-a

TEST_F(ObPartitionMigratorTest, update_base_source_output_newer)
{
  init();
  ObMigrateInfoFetchResult candidate;
  ObMigrateInfoFetchResult output;
  ObMigrateStoreInfo fake_info;
  // 3-a         2-b                 3-a
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(2, b_, 100, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 300, ObVersion(4), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));

  // 3-a,4-a  2-b,3-b           3-a,4-a
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(3, b_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 400, ObVersion(5), output));
  ASSERT_EQ(2, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, a_));

  // 3-a,        2-b,3-b,4-b       3-a,4-b
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 250, ObVersion(4), output));
  ASSERT_EQ(2, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, b_));

  // 3-a,4-b     2-c,3-c,4-c,5-c   3-a,4-b,5-c
  candidate.info_list_.reset();
  make_info(2, c_, 100, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(3, c_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(4, c_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(5, c_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 350, ObVersion(5), output));
  ASSERT_EQ(3, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, b_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 2, 5, c_));

  // 5-a          3-b,4-b            5-a
  output.info_list_.reset();
  make_info(5, a_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(3, b_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 500, ObVersion(6), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 5, a_));
}
// output      candidate         new_output
// 3-a         4-b               4-b
// 3-a,4-a     4-b,5-b           4-b,5-b
// 3-a,4-a,5-c 4-b               4-b,5-c
// 3-a,4-b     4-c               4-c
// 3-b         5-c               5-c
// 3-a(200)    4-b(300)          3-a        active_memstore:4 max_confirm_log_id:350
// 3-a(200)    4-b(300)          4-b        active_memstore:5 max_confirm_log_id:450
TEST_F(ObPartitionMigratorTest, update_base_source_candidate_newer)
{
  init();
  ObMigrateInfoFetchResult candidate;
  ObMigrateInfoFetchResult output;
  ObMigrateStoreInfo fake_info;
  // 3-a         4-b               4-b
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 250, ObVersion(4), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 4, b_));

  // 3-a,4-a     4-b,5-b           4-b,5-b
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 350, ObVersion(5), output));
  ASSERT_EQ(2, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 4, b_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 5, b_));

  // 3-a,4-a,5-c 4-b               4-b,5-c
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(5, c_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 500, ObVersion(6), output));
  ASSERT_EQ(2, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 4, b_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 5, c_));

  // 3-a,4-b     4-c               4-c
  output.info_list_.reset();
  make_info(3, c_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(4, c_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 400, ObVersion(5), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 4, c_));

  // 3-b         5-c               5-c
  output.info_list_.reset();
  make_info(3, b_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(5, c_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 300, ObVersion(4), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 5, c_));

  // 3-a(200)    4-b(300)          3-a        active_memstore:4 max_confirm_log_id:350
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 350, ObVersion(4), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));

  // 3-a(200)    4-b(300)          4-b        active_memstore:5 max_confirm_log_id:450
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 450, ObVersion(5), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 4, b_));
}

TEST_F(ObPartitionMigratorTest, update_base_source_sstable_not_same)
{
  init();
  ObMigrateInfoFetchResult candidate;
  ObMigrateInfoFetchResult output;
  ObMigrateStoreInfo fake_info;

  // output has more sstables
  // 3-a(more)   3-b      3-a
  make_info(3, a_, 200, fake_info);
  fake_info.store_info_.sstable_count_ = 3;
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(3, b_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 300, ObVersion(4), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));

  // output has more sstables
  // 3-a(more)   4-b     4-b
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  fake_info.store_info_.sstable_count_ = 3;
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 250, ObVersion(4), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 4, b_));

  // candidate has more sstables
  // 3-a   3-b(more)      3-b
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(3, b_, 200, fake_info);
  fake_info.store_info_.sstable_count_ = 3;
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 300, ObVersion(4), output));
  ASSERT_EQ(1, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, b_));

  // candidate has more sstables
  // 3-a,4-a   3-b(more)      3-b,4-a
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(3, b_, 200, fake_info);
  fake_info.store_info_.sstable_count_ = 3;
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 400, ObVersion(5), output));
  ASSERT_EQ(2, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, b_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, a_));

  // candidate has more sstables
  // 3-a,4-a   3-b(more),4-b      3-b,4-a
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(3, b_, 200, fake_info);
  fake_info.store_info_.sstable_count_ = 3;
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 400, ObVersion(5), output));
  ASSERT_EQ(2, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, b_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, a_));

  // candidate has more sstables
  // 3-a(more)   2-b,3-b,4-b      3-a,4-b
  output.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  fake_info.store_info_.sstable_count_ = 3;
  ASSERT_EQ(OB_SUCCESS, output.info_list_.push_back(fake_info));
  candidate.info_list_.reset();
  make_info(2, b_, 100, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(3, b_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  make_info(4, b_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, candidate.info_list_.push_back(fake_info));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMigrateTask::update_base_source(candidate, 250, ObVersion(4), output));
  ASSERT_EQ(2, output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(output, 0, 3, a_));
  ASSERT_EQ(OB_SUCCESS, check_info(output, 1, 4, b_));
}

TEST_F(ObPartitionMigratorTest, update_construct_source)
{
  init();
  ObMigrateInfoFetchResult info_local;
  ObMigrateInfoFetchResult info_src;
  ObMigrateInfoFetchResult info_output;
  ObMigrateStoreInfo fake_info;

  info_local.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(5, a_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));

  info_src.info_list_.reset();
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));

  ASSERT_EQ(OB_SUCCESS,
      ObPartitionMigrateTask::construct_source(info_local, 600, ObVersion(6), info_src, info_src, info_output));
  ASSERT_EQ(1, info_output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(info_output, 0, 5, b_));

  info_local.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(5, a_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));

  info_src.info_list_.reset();
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(6, b_, 500, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(7, b_, 550, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));

  ASSERT_EQ(OB_SUCCESS,
      ObPartitionMigrateTask::construct_source(info_local, 600, ObVersion(6), info_src, info_src, info_output));
  ASSERT_EQ(1, info_output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(info_output, 0, 5, b_));

  info_local.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(5, a_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));

  info_src.info_list_.reset();
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(6, b_, 500, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));

  ASSERT_EQ(OB_SUCCESS,
      ObPartitionMigrateTask::construct_source(info_local, 450, ObVersion(6), info_src, info_src, info_output));
  ASSERT_EQ(2, info_output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(info_output, 0, 5, b_));
  ASSERT_EQ(OB_SUCCESS, check_info(info_output, 1, 6, b_));

  info_local.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(5, a_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));

  info_src.info_list_.reset();
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(6, b_, 450, fake_info);
  fake_info.store_info_.saved_storage_info_.memstore_version_ = ObVersion(6, 1);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));

  ASSERT_EQ(OB_SUCCESS,
      ObPartitionMigrateTask::construct_source(info_local, 480, ObVersion(6), info_src, info_src, info_output));
  ASSERT_EQ(1, info_output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(info_output, 0, 5, b_));

  info_local.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));

  info_src.info_list_.reset();
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(6, b_, 500, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(7, b_, 600, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(8, b_, 700, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(9, b_, 750, fake_info);
  fake_info.store_info_.saved_storage_info_.memstore_version_ = ObVersion(6, 1);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));

  ASSERT_EQ(OB_SUCCESS,
      ObPartitionMigrateTask::construct_source(info_local, 800, ObVersion(6), info_src, info_src, info_output));
  ASSERT_EQ(1, info_output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(info_output, 0, 5, b_));

  info_local.info_list_.reset();
  make_info(3, a_, 200, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(4, a_, 300, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(5, a_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));
  make_info(6, a_, 410, fake_info);
  fake_info.store_info_.saved_storage_info_.memstore_version_ = ObVersion(6, 1);
  ASSERT_EQ(OB_SUCCESS, info_local.info_list_.push_back(fake_info));

  info_src.info_list_.reset();
  make_info(5, b_, 400, fake_info);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));
  make_info(6, b_, 450, fake_info);
  fake_info.store_info_.saved_storage_info_.memstore_version_ = ObVersion(6, 1);
  ASSERT_EQ(OB_SUCCESS, info_src.info_list_.push_back(fake_info));

  ASSERT_EQ(OB_SUCCESS,
      ObPartitionMigrateTask::construct_source(info_local, 420, ObVersion(6), info_src, info_src, info_output));
  ASSERT_EQ(2, info_output.info_list_.count());
  ASSERT_EQ(OB_SUCCESS, check_info(info_output, 0, 5, b_));
  ASSERT_EQ(OB_SUCCESS, check_info(info_output, 1, 6, b_));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test partition migrator");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
