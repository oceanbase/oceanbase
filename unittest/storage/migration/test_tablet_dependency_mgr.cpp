/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <cstdint>
#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "lib/ob_errno.h"
#include "lib/hash/ob_hashset.h"
#include "test_tablet_dependency_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace hash;

namespace unittest
{
int TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(ObTabletCopyDependencyMgr &dep_mgr, const ObIArray<ObLogicTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  for (int idx = 0; idx < tablet_ids.count() && OB_SUCC(ret); ++idx) {
    const ObLogicTabletID &tablet_id = tablet_ids.at(idx);
    if (OB_FAIL(dep_mgr.remove_tablet_dependency(tablet_id.tablet_id_))) {
      LOG_WARN("failed to remove tablet dependency", K(ret), K(tablet_id));
    }
  }
  return ret;
}

void TabletDependencyMgrUtils::update_and_check_expected_tablet_count(ObTabletCopyDependencyMgr &dep_mgr,
    const ObIArray<ObLogicTabletID> &tablet_ids, int64_t &expected_tablet_count)
{
  expected_tablet_count -= tablet_ids.count();
  ASSERT_EQ(expected_tablet_count, dep_mgr.get_tablet_count());
}

class ObTabletCopyDependencyMgrTest : public ::testing::Test
{
public:
  ObTabletCopyDependencyMgrTest();
  virtual ~ObTabletCopyDependencyMgrTest();
  virtual void SetUp();
  virtual void TearDown();
public:
  ObTabletCopyDependencyMgr dep_mgr_;
};

struct TabletInfo final
{
  ObTabletID tablet_id_;
  int64_t transfer_seq_;
  ObCopyTabletStatus::STATUS status_;
  int64_t data_size_;
};

ObTabletCopyDependencyMgrTest::ObTabletCopyDependencyMgrTest()
{
}

ObTabletCopyDependencyMgrTest::~ObTabletCopyDependencyMgrTest()
{
}

void ObTabletCopyDependencyMgrTest::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.init());
}

void ObTabletCopyDependencyMgrTest::TearDown()
{
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.destroy());
}

TEST_F(ObTabletCopyDependencyMgrTest, test_init)
{
  // init twice
  ASSERT_EQ(OB_INIT_TWICE, dep_mgr_.init());

  // init after reset
  dep_mgr_.reset();
  ASSERT_EQ(OB_INIT_TWICE, dep_mgr_.init());

  // init after destory
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.destroy());
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.init());
}

TEST_F(ObTabletCopyDependencyMgrTest, test_simple_add_and_fetch)
{
  const int64_t TEST_TABLET_COUNT = 3;
  const int64_t MAX_TABLET_SIZE = INT64_MAX;
  int64_t expected_tablet_count = TEST_TABLET_COUNT;
  TabletInfo infos[TEST_TABLET_COUNT] = {
    {ObTabletID(1), 0, ObCopyTabletStatus::TABLET_EXIST, 100},
    {ObTabletID(2), 1, ObCopyTabletStatus::TABLET_EXIST, 200},
    {ObTabletID(3), 2, ObCopyTabletStatus::TABLET_EXIST, 300}
  };

  // add tablets
  for (int idx = 0; idx < TEST_TABLET_COUNT; ++idx) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.add_independent_tablet(
      infos[idx].tablet_id_, infos[idx].transfer_seq_, infos[idx].status_, infos[idx].data_size_));
  }

  // refresh tablets
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.refresh_ready_tablets());
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // fetch 2 tablets
  ObArray<ObLogicTabletID> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(2, MAX_TABLET_SIZE, tablet_ids));

  ASSERT_EQ(2, tablet_ids.count());
  ASSERT_EQ(OB_SUCCESS, TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(dep_mgr_, tablet_ids));
  TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);

  // fetch 2 tablets again, only one tablet left, so only one will be fetched
  tablet_ids.reset();
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(2, MAX_TABLET_SIZE, tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(OB_SUCCESS, TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(dep_mgr_, tablet_ids));
  TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);

  // fetch 2 tablets again, all tablets are already fetched, so no tablet will be fetched
  tablet_ids.reset();
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(2, MAX_TABLET_SIZE, tablet_ids));
  ASSERT_EQ(0, tablet_ids.count());
  ASSERT_EQ(OB_SUCCESS, TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(dep_mgr_, tablet_ids));
  TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);

  bool is_done = false;
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.check_is_done(is_done));
  ASSERT_TRUE(is_done);
}

TEST_F(ObTabletCopyDependencyMgrTest, test_add_and_fetch_with_size_threshold)
{
  const int64_t TEST_TABLET_COUNT = 6;
  int64_t expected_tablet_count = TEST_TABLET_COUNT;
  TabletInfo infos[TEST_TABLET_COUNT] = {
    {ObTabletID(1), 0, ObCopyTabletStatus::TABLET_EXIST, 100},
    {ObTabletID(2), 1, ObCopyTabletStatus::TABLET_EXIST, 200},
    {ObTabletID(3), 2, ObCopyTabletStatus::TABLET_EXIST, 500},
    {ObTabletID(4), 3, ObCopyTabletStatus::TABLET_EXIST, 200},
    {ObTabletID(5), 4, ObCopyTabletStatus::TABLET_EXIST, 300},
    {ObTabletID(6), 5, ObCopyTabletStatus::TABLET_EXIST, 100}
  };

  // add tablets
  for (int idx = 0; idx < TEST_TABLET_COUNT; ++idx) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.add_independent_tablet(
      infos[idx].tablet_id_, infos[idx].transfer_seq_, infos[idx].status_, infos[idx].data_size_));
  }

  // refresh tablets
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.refresh_ready_tablets());
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // fetch tablets with size threshold
  const int64_t TEST_SIZE_THRESHOLD = 400;
  const int64_t BUCKET_NUM = 64;
  ObArray<ObLogicTabletID> tablet_ids;
  ObHashSet<ObTabletID> tablet_set;

  ASSERT_EQ(OB_SUCCESS, tablet_set.create(BUCKET_NUM));

  bool is_done = false;
  // fetch tablets until all tablets are fetched
  while (!is_done) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.check_is_done(is_done));
    if (is_done) {
      break;
    } else {
      tablet_ids.reset();
      ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(TEST_TABLET_COUNT, TEST_SIZE_THRESHOLD, tablet_ids));
      ARRAY_FOREACH_NORET(tablet_ids, idx)
      {
        ObTabletID tablet_id = tablet_ids.at(idx).tablet_id_;
        ASSERT_EQ(OB_SUCCESS, tablet_set.set_refactored(tablet_id));
      }
      ASSERT_EQ(OB_SUCCESS, TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(dep_mgr_, tablet_ids));
      TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);
    }
  }

  ASSERT_EQ(TEST_TABLET_COUNT, tablet_set.size());

  // assert all tablets are fetched
  for (int idx = 0; idx < TEST_TABLET_COUNT; ++idx) {
    ASSERT_EQ(OB_HASH_EXIST, tablet_set.exist_refactored(infos[idx].tablet_id_));
  }

  // after all tablets are fetched, fetch again, no tablet will be fetched
  tablet_ids.reset();
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(TEST_TABLET_COUNT, TEST_SIZE_THRESHOLD, tablet_ids));
  ASSERT_EQ(0, tablet_ids.count());
  TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);
}

TEST_F(ObTabletCopyDependencyMgrTest, test_dependency_chain)
{
  const int64_t TEST_TABLET_COUNT = 9;
  const int64_t MAX_TABLET_SIZE = INT64_MAX;
  int64_t expected_tablet_count = TEST_TABLET_COUNT;
  const ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::TABLET_EXIST;
  const int64_t transfer_seq = 0;
  const int64_t data_size = 100;

  // add dependent tablets
  // #1 (fake) -> #2 -> #3 -> #4 -> #5 -> #6 -> #7 -> #8 -> #9
  for (int idx = 1; idx <= TEST_TABLET_COUNT - 1; ++idx) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.add_dependent_tablet_pair(ObTabletID(idx+1), ObTabletID(idx), transfer_seq, status, data_size));
  }

  // refresh tablets
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.refresh_ready_tablets());
  expected_tablet_count -= 1;
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // fetch and remove dependency
  ObArray<ObLogicTabletID> tablet_ids;

  // for each time, fetch one tablet and remove its dependency
  // can fetch #2 the first time, #3 the second time, and so on
  for (int idx = 1; idx <= TEST_TABLET_COUNT - 1; ++idx) {
    tablet_ids.reset();
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(1, MAX_TABLET_SIZE, tablet_ids));
    ASSERT_EQ(1, tablet_ids.count());
    ASSERT_EQ(ObTabletID(idx + 1), tablet_ids.at(0).tablet_id_);
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.remove_tablet_dependency(ObTabletID(idx + 1)));
    TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);
  }

  // all tablets are fetched and removed
  bool is_done = false;
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.check_is_done(is_done));
  ASSERT_TRUE(is_done);
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());
}

TEST_F(ObTabletCopyDependencyMgrTest, test_multiple_dependency)
{
  const int64_t TEST_TABLET_COUNT = 8;
  const int64_t MAX_TABLET_SIZE = INT64_MAX;
  int64_t expected_tablet_count = TEST_TABLET_COUNT;
  const ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::TABLET_EXIST;
  const int64_t transfer_seq = 0;

  // add dependent tablets
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.add_independent_tablet(ObTabletID(1), transfer_seq, status, 100));

  // #1 -> #2,#3,#4,#5,#6,#7,#8
  for (int idx = 2; idx <= TEST_TABLET_COUNT; ++idx) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.add_dependent_tablet_pair(ObTabletID(idx), ObTabletID(1), transfer_seq, status, 100));
  }

  // refresh tablets
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.refresh_ready_tablets());
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // fetch and remove dependency
  ObArray<ObLogicTabletID> tablet_ids;

  // can fetch #1 the first time
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(TEST_TABLET_COUNT, MAX_TABLET_SIZE, tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(ObTabletID(1), tablet_ids.at(0).tablet_id_);
  ASSERT_EQ(OB_SUCCESS, TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(dep_mgr_, tablet_ids));
  TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);

  const int64_t BUCKET_NUM = 64;
  ObHashSet<ObTabletID> tablet_set;

  ASSERT_EQ(OB_SUCCESS, tablet_set.create(BUCKET_NUM));

  // fetch and remove dependency
  bool is_done = false;
  while (!is_done) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.check_is_done(is_done));
    if (is_done) {
      break;
    } else {
      tablet_ids.reset();
      ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(TEST_TABLET_COUNT, MAX_TABLET_SIZE, tablet_ids));
      ARRAY_FOREACH_NORET(tablet_ids, idx)
      {
        ObTabletID tablet_id = tablet_ids.at(idx).tablet_id_;
        ASSERT_EQ(OB_SUCCESS, tablet_set.set_refactored(tablet_id));
      }
      ASSERT_EQ(OB_SUCCESS, TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(dep_mgr_, tablet_ids));
      TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);
    }
  }

  ASSERT_EQ(TEST_TABLET_COUNT - 1, tablet_set.size());
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // assert all tablets are fetched
  for (int idx = 2; idx <= TEST_TABLET_COUNT; ++idx) {
    ASSERT_EQ(OB_HASH_EXIST, tablet_set.exist_refactored(ObTabletID(idx)));
  }
}

TEST_F(ObTabletCopyDependencyMgrTest, test_multiple_dependency_with_fake)
{
  const int64_t TEST_TABLET_COUNT = 7;
  const int64_t MAX_TABLET_SIZE = INT64_MAX;
  int64_t expected_tablet_count = TEST_TABLET_COUNT;
  const ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::TABLET_EXIST;
  const int64_t transfer_seq = 0;

  // #1(fake) -> #2,#3,#4,#5,#6,#7,#8
  for (int idx = 2; idx <= TEST_TABLET_COUNT + 1; ++idx) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.add_dependent_tablet_pair(ObTabletID(idx), ObTabletID(1), transfer_seq, status, 100));
  }

  // refresh tablets
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.refresh_ready_tablets());
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // fetch and remove dependency
  ObArray<ObLogicTabletID> tablet_ids;
  const int64_t BUCKET_NUM = 64;
  ObHashSet<ObTabletID> tablet_set;
  ASSERT_EQ(OB_SUCCESS, tablet_set.create(BUCKET_NUM));

  // fetch and remove dependency
  bool is_done = false;
  while (!is_done) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.check_is_done(is_done));
    if (is_done) {
      break;
    } else {
      tablet_ids.reset();
      ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(TEST_TABLET_COUNT, MAX_TABLET_SIZE, tablet_ids));
      ARRAY_FOREACH_NORET(tablet_ids, idx)
      {
        ObTabletID tablet_id = tablet_ids.at(idx).tablet_id_;
        ASSERT_EQ(OB_SUCCESS, tablet_set.set_refactored(tablet_id));
      }
      ASSERT_EQ(OB_SUCCESS, TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(dep_mgr_, tablet_ids));
      TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);
    }
  }

  ASSERT_EQ(TEST_TABLET_COUNT, tablet_set.size());
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // assert all tablets are fetched
  for (int idx = 2; idx <= TEST_TABLET_COUNT + 1; ++idx) {
    ASSERT_EQ(OB_HASH_EXIST, tablet_set.exist_refactored(ObTabletID(idx)));
  }
}

TEST_F(ObTabletCopyDependencyMgrTest, test_multiple_dependency_multilayer)
{
  // dependency tree (layer 8, child 2, binary tree)
  const int64_t LAYER_COUNT = 8;
  const int64_t CHILD_COUNT = 2;
  const int64_t TEST_TALBET_COUNT = (1 << LAYER_COUNT) - 1;
  const int64_t MAX_TABLET_SIZE = INT64_MAX;
  int64_t expected_tablet_count = TEST_TALBET_COUNT;

  const ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::TABLET_EXIST;
  const int64_t transfer_seq = 0;
  const int64_t data_size = 100;

  // add independent tablet
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.add_independent_tablet(ObTabletID(1), transfer_seq, status, data_size));

  // add dependent tablets
  for (int idx = 2; idx <= TEST_TALBET_COUNT; ++idx) {
    int parent_idx = idx >> 1;
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.add_dependent_tablet_pair(ObTabletID(idx), ObTabletID(parent_idx), transfer_seq, status, data_size));
  }

  // refresh tablets
  ASSERT_EQ(OB_SUCCESS, dep_mgr_.refresh_ready_tablets());
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // fetch and remove dependency
  ObArray<ObLogicTabletID> tablet_ids;
  const int64_t BUCKET_NUM = 64;
  ObHashSet<ObTabletID> tablet_set;
  ASSERT_EQ(OB_SUCCESS, tablet_set.create(BUCKET_NUM));

  // fetch and remove dependency
  int cnt = 1;
  bool is_done = false;
  while (!is_done) {
    ASSERT_EQ(OB_SUCCESS, dep_mgr_.check_is_done(is_done));
    if (is_done) {
      break;
    } else {
      tablet_ids.reset();
      ASSERT_EQ(OB_SUCCESS, dep_mgr_.fetch_ready_tablet_group(TEST_TALBET_COUNT, MAX_TABLET_SIZE, tablet_ids));
      ASSERT_EQ(1 << (cnt - 1), tablet_ids.count());
      ARRAY_FOREACH_NORET(tablet_ids, idx)
      {
        ObTabletID tablet_id = tablet_ids.at(idx).tablet_id_;
        ASSERT_EQ(OB_SUCCESS, tablet_set.set_refactored(tablet_id));
      }
      ASSERT_EQ(OB_SUCCESS, TabletDependencyMgrUtils::remove_tablets_from_dep_mgr(dep_mgr_, tablet_ids));
      TabletDependencyMgrUtils::update_and_check_expected_tablet_count(dep_mgr_, tablet_ids, expected_tablet_count);
        ++cnt;
    }
  }

  ASSERT_EQ(TEST_TALBET_COUNT, tablet_set.size());
  ASSERT_EQ(expected_tablet_count, dep_mgr_.get_tablet_count());

  // assert all tablets are fetched
  for (int idx = 2; idx <= TEST_TALBET_COUNT; ++idx) {
    ASSERT_EQ(OB_HASH_EXIST, tablet_set.exist_refactored(ObTabletID(idx)));
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_dependency_mgr.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tablet_dependency_mgr.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}