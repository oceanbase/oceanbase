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

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define private public
#define protected public
#include "storage/compaction/ob_uncommit_tx_info.h"
#include "mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
using namespace compaction;
using namespace common;
using namespace hash;

namespace storage {
class TestUncommitTxInfo : public ::testing::Test
{
public:
  TestUncommitTxInfo() = default;
  virtual ~TestUncommitTxInfo() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  public:
  ObUncommitTxInfoCollector uncommit_tx_collector_;
};

void TestUncommitTxInfo::SetUp()
{
  int ret = OB_SUCCESS;
  uncommit_tx_collector_.uncommit_tx_info_.reuse();
  ret = uncommit_tx_collector_.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestUncommitTxInfo::TearDown()
{
  uncommit_tx_collector_.uncommit_tx_info_.reset();
  uncommit_tx_collector_.reset();
}

}
namespace unittest{
TEST_F(TestUncommitTxInfo, test_record_seq_single_array)
{
  int ret = OB_SUCCESS;
  int64_t count = 20;
  for (int64_t i = 1; i <= count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i);
    ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(tx_seq_key));
  }
  ASSERT_EQ(20, uncommit_tx_collector_.uncommit_tx_info_.tx_infos_.count());
  ASSERT_EQ(ObBasicUncommitTxInfo::RECORD_SEQ, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
  ASSERT_EQ(1, uncommit_tx_collector_.uncommit_tx_info_.version_);
  ASSERT_TRUE(uncommit_tx_collector_.uncommit_tx_info_.is_valid());
}

TEST_F(TestUncommitTxInfo, test_record_seq_several_arrayss)
{
  int ret = OB_SUCCESS;
  int64_t count = 10;
  ObMemUncommitTxInfo info1;
  ObMemUncommitTxInfo info2;
  ObMemUncommitTxInfo info3;
  info1.reuse();
  info2.reuse();
  info3.reuse();
  for (int64_t i = 1; i <= count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i);
    ASSERT_EQ(OB_SUCCESS, info1.push_back(tx_seq_key));
  }
  ASSERT_EQ(10, info1.tx_infos_.count());
  for (int64_t i = 11; i <= 10 + count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i);
    ASSERT_EQ(OB_SUCCESS, info2.push_back(tx_seq_key));
  }
  ASSERT_EQ(10, info2.tx_infos_.count());
  for (int64_t i = 21; i <= 20 + count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i);
    ASSERT_EQ(OB_SUCCESS, info3.push_back(tx_seq_key));
  }
  ASSERT_EQ(10, info3.tx_infos_.count());
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(info1));
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(info2));
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(info3));
  ASSERT_EQ(30, uncommit_tx_collector_.uncommit_tx_info_.tx_infos_.count());
  ASSERT_EQ(ObBasicUncommitTxInfo::RECORD_SEQ, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
  ASSERT_EQ(1, uncommit_tx_collector_.uncommit_tx_info_.version_);
  ASSERT_TRUE(uncommit_tx_collector_.uncommit_tx_info_.is_valid());
}

TEST_F(TestUncommitTxInfo, test_only_tx_id_single_array)
{
  int ret = OB_SUCCESS;
  int64_t count = 40;
  for (int64_t i = 1; i <= count; ++i ) {
    ObUncommitTxDesc tx_seq_key(10,i);
    ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(tx_seq_key));
  }
  ASSERT_EQ(1, uncommit_tx_collector_.uncommit_tx_info_.tx_infos_.count());
  ASSERT_EQ(ObBasicUncommitTxInfo::ONLY_TXID, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
  ASSERT_EQ(1, uncommit_tx_collector_.uncommit_tx_info_.version_);
  ASSERT_TRUE(uncommit_tx_collector_.uncommit_tx_info_.is_valid());
}

TEST_F(TestUncommitTxInfo, test_only_tx_id_sevaral_arrays)
{
  int ret = OB_SUCCESS;
  int64_t count = 10;
  ObMemUncommitTxInfo info1;
  ObMemUncommitTxInfo info2;
  ObMemUncommitTxInfo info3;
  info1.set_info_status_only_tx_id();
  for (int64_t i = 1; i <= count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i, 0);
    ASSERT_EQ(OB_SUCCESS, info1.push_back(tx_seq_key));
  }
  info1.total_uncommit_row_count_ = 42;
  info2.set_info_status_only_tx_id();
  for (int64_t i = 11; i <= 10 + count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i, 0);
    ASSERT_EQ(OB_SUCCESS, info2.push_back(tx_seq_key));
  }
  info2.total_uncommit_row_count_ = 42;
  info3.set_info_status_only_tx_id();
  for (int64_t i = 21; i <= 20 + count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i, 0);
    ASSERT_EQ(OB_SUCCESS, info3.push_back(tx_seq_key));
  }
  info3.total_uncommit_row_count_ = 42;
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(info1));
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(info2));
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(info3));
  ASSERT_EQ(30, uncommit_tx_collector_.uncommit_tx_info_.tx_infos_.count());
  ASSERT_EQ(126, uncommit_tx_collector_.uncommit_tx_info_.total_uncommit_row_count_);
  ASSERT_EQ(ObBasicUncommitTxInfo::ONLY_TXID, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
  ASSERT_EQ(1, uncommit_tx_collector_.uncommit_tx_info_.version_);
  ASSERT_TRUE(uncommit_tx_collector_.uncommit_tx_info_.is_valid());
}

TEST_F(TestUncommitTxInfo, test_info_overflow_single_array)
{
  int ret = OB_SUCCESS;
  int64_t count = 40;
  for (int64_t i = 1; i <= count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i);
    ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(tx_seq_key));
  }
  ASSERT_EQ(0, uncommit_tx_collector_.uncommit_tx_info_.tx_infos_.count());
  ASSERT_EQ(ObBasicUncommitTxInfo::INFO_OVERFLOW, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
  ASSERT_EQ(1, uncommit_tx_collector_.uncommit_tx_info_.version_);
  ASSERT_TRUE(uncommit_tx_collector_.uncommit_tx_info_.is_valid());
}

TEST_F(TestUncommitTxInfo, test_info_overflow_several_arrays)
{
  int ret = OB_SUCCESS;
  int64_t count = 20;
  ObUncommitTxInfoCollector uncommit_tx_collector1;
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector1.init(1));
  for (int64_t i = 1; i <= count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i);
    ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector1.push_back(tx_seq_key));
  }
  ASSERT_EQ(ObBasicUncommitTxInfo::RECORD_SEQ, uncommit_tx_collector1.uncommit_tx_info_.info_status_);
  count = 40;
  ObUncommitTxInfoCollector uncommit_tx_collector2;
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector2.init(1));
  for (int64_t i = 21; i <= 20 + count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i);
    ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector2.push_back(tx_seq_key));
  }
  ASSERT_EQ(ObBasicUncommitTxInfo::INFO_OVERFLOW, uncommit_tx_collector2.uncommit_tx_info_.info_status_);
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(uncommit_tx_collector1.uncommit_tx_info_));
  ASSERT_EQ(ObBasicUncommitTxInfo::RECORD_SEQ, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
  ASSERT_EQ(OB_SIZE_OVERFLOW, uncommit_tx_collector_.push_back(uncommit_tx_collector2.uncommit_tx_info_));
  ASSERT_EQ(0, uncommit_tx_collector_.uncommit_tx_info_.tx_infos_.count());
  ASSERT_EQ(ObBasicUncommitTxInfo::INFO_OVERFLOW, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
  ASSERT_EQ(1, uncommit_tx_collector_.uncommit_tx_info_.version_);
  ASSERT_TRUE(uncommit_tx_collector_.uncommit_tx_info_.is_valid());
}

TEST_F(TestUncommitTxInfo, test_commit_version_single_array)
{
  int ret = OB_SUCCESS;
  int64_t count = 20;
  for (int64_t i = 1; i <= count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i,ObUncommitTxDesc::COMMIT_VERSION);
    ASSERT_EQ(ObUncommitTxDesc::COMMIT_VERSION, tx_seq_key.key_status_);
    ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(tx_seq_key));
  }
  ASSERT_EQ(20, uncommit_tx_collector_.uncommit_tx_info_.tx_infos_.count());
  ASSERT_EQ(ObBasicUncommitTxInfo::RECORD_SEQ, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
  ASSERT_EQ(1, uncommit_tx_collector_.uncommit_tx_info_.version_);
  ASSERT_TRUE(uncommit_tx_collector_.uncommit_tx_info_.is_valid());
}


TEST_F(TestUncommitTxInfo, test_convert_to_only_txid)
{
  int ret = OB_SUCCESS;
  int64_t count = 10;
  ObMemUncommitTxInfo info1;
  ObMemUncommitTxInfo info2;
  for (int64_t i = 11; i <= 10 + count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,i);
    ASSERT_EQ(OB_SUCCESS, info1.push_back(tx_seq_key));
  }
  info1.set_info_status_record_seq();
  info1.total_uncommit_row_count_ = 10;
  for (int64_t i = 11; i <= 10 + count; ++i ) {
    ObUncommitTxDesc tx_seq_key(i,0);
    ASSERT_EQ(OB_SUCCESS, info2.push_back(tx_seq_key));
  }
  info2.set_info_status_only_tx_id();
  info2.total_uncommit_row_count_ = 42;
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(info1));
  ASSERT_EQ(OB_SUCCESS, uncommit_tx_collector_.push_back(info2));
  ASSERT_EQ(10, uncommit_tx_collector_.uncommit_tx_info_.tx_infos_.count());
  ASSERT_EQ(52, uncommit_tx_collector_.uncommit_tx_info_.total_uncommit_row_count_);
  ASSERT_EQ(ObBasicUncommitTxInfo::ONLY_TXID, uncommit_tx_collector_.uncommit_tx_info_.info_status_);
}

} //end namespace unittest
} //end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_uncommit_tx_info.log*");
  OB_LOGGER.set_file_name("test_uncommit_tx_info.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}