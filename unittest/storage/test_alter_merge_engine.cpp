/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/ob_cluster_version.h"
#include "share/schema/ob_merge_engine_upper_version.h"

namespace oceanbase
{
using namespace share;
using namespace schema;

namespace unittest
{

class ObAlterMergeEngineTest : public ::testing::Test
{
public:
void TearDown()
{
  upper_version_.reset();
}
void check_init_upper_version(const ObMergeEngineType merge_engine_type)
{
  ASSERT_EQ(true, upper_version_.is_valid());
  switch (merge_engine_type) {
    case ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE: {
      ASSERT_EQ(upper_version_.upper_versions_[0], share::SCN::max_scn());
      ASSERT_EQ(upper_version_.upper_versions_[1], share::SCN::min_scn());
      ASSERT_EQ(upper_version_.upper_versions_[2], share::SCN::min_scn());
      break;
    }
    case ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT: {
      ASSERT_EQ(upper_version_.upper_versions_[0], share::SCN::min_scn());
      ASSERT_EQ(upper_version_.upper_versions_[1], share::SCN::max_scn());
      ASSERT_EQ(upper_version_.upper_versions_[2], share::SCN::min_scn());
      break;
    }
    case ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY: {
      ASSERT_EQ(upper_version_.upper_versions_[0], share::SCN::min_scn());
      ASSERT_EQ(upper_version_.upper_versions_[1], share::SCN::min_scn());
      ASSERT_EQ(upper_version_.upper_versions_[2], share::SCN::max_scn());
      break;
    }
    default: {
      ASSERT_EQ(1, 0);
      break;
    }
  }
}

void check_update_upper_version(const SCN &cur_scn, const std::vector<SCN> scn_array)
{
  printf("------check cur_scn: %llu------\n", cur_scn.ts_ns_);
  ASSERT_EQ(true, upper_version_.is_valid());
  int64_t max_idx = static_cast<int64_t>(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN);
  for (int i = 0; i < max_idx; ++i) {
    ASSERT_EQ(upper_version_.upper_versions_[i], scn_array[i]);
  }
}
public:
  ObMergeEngineUpperVersion upper_version_;
};

TEST_F(ObAlterMergeEngineTest, test_init_upper_version)
{
  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  check_init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_ERR_UNEXPECTED, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  check_init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  check_init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY);
  upper_version_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN));
  upper_version_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_MAX));
  upper_version_.reset();
}

TEST_F(ObAlterMergeEngineTest, test_update_upper_version)
{
  uint64_t data_version = DATA_VERSION_5_0_1_0;
  SCN scn0, scn1, scn2, scn3;
  scn0.set_min();
  scn1.set_min();
  scn2.set_min();
  scn3.set_min();

  upper_version_.reset();
  upper_version_.set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  scn0.ts_ns_ = 1;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn1.ts_ns_ = 1; scn2 = SCN::max_scn(); scn3 = SCN::min_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn0.ts_ns_ = 2;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2 = SCN::min_scn(); scn3 = SCN::min_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 3;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn1.ts_ns_ = 3; scn2 = SCN::max_scn(); scn3 = SCN::min_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 4;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1.ts_ns_ = 3; scn2.ts_ns_ = 4; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 5;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn1.ts_ns_ = 3; scn2 = SCN::max_scn(); scn3.ts_ns_ = 5;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 6;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2.ts_ns_ = 6; scn3.ts_ns_ = 5;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 7;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1.ts_ns_ = 7; scn2.ts_ns_ = 6; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 8;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2.ts_ns_ = 6; scn3.ts_ns_ = 8;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn0.ts_ns_ = 9;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn1 = SCN::min_scn(); scn2 = SCN::max_scn(); scn3.ts_ns_ = 9;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 10;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1 = SCN::min_scn(); scn2.ts_ns_ = 10; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 11;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2.ts_ns_ = 10; scn3.ts_ns_ = 11;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn0.ts_ns_ = 12;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1 = SCN::min_scn(); scn2.ts_ns_ = 12; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 13;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2.ts_ns_ = 12; scn3.ts_ns_ = 13;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 14;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(data_version, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1.ts_ns_ = 14; scn2.ts_ns_ = 12; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  printf("start running test\n");
  return RUN_ALL_TESTS();
}
