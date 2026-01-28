/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>

#define private public
#define protected public

#include "mtlenv/storage/medium_info_common.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "storage/tablet/ob_tablet.h"
#include "unittest/storage/ob_ttl_filter_info_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;

namespace oceanbase
{
using namespace compaction;
namespace storage
{

class TestTTLFilterInfoReader : public MediumInfoCommon
{
public:
  TestTTLFilterInfoReader() = default;
  virtual ~TestTTLFilterInfoReader() = default;
};

TEST_F(TestTTLFilterInfoReader, read_multi_ttl_filter_info_from_minor)
{
  int ret = OB_SUCCESS;
  bool equal = false;

  const char *key_data = "tx_id    commit_ver  filter_type   filter_value   filter_col\n"
                         "1        1000          1           1              0         \n"
                         "2        -2000         1           1000           1         \n"
                         "3        3000          1           2000           2         \n";

  // create tablet
  const ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTTLFilterInfoArray mock_array;
  ObTTLFilterInfoArray ttl_filter_info_array;
  ObMdsReadInfoCollector collector;
  ObTabletHandle tablet_handle;

  ASSERT_EQ(OB_SUCCESS, create_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, TTLFilterInfoHelper::batch_mock_ttl_filter_info_without_sort(allocator_, key_data, mock_array));
  ASSERT_EQ(3, mock_array.count());

  // insert data into mds table
  ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(0)));

  // exist new node in mds_table
  ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS,
            tablet_handle.get_obj()->inner_read_info_array_from_mds<ObTTLFilterInfoKey>(
                allocator_, ObVersionRange(100, 200), ttl_filter_info_array, collector));
  tablet_handle.reset();

  ASSERT_EQ(true, collector.exist_new_committed_node_);
  ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(0)->commit_version_));
  ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(1)));
  ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(2)));
  ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(2)->commit_version_));

  // check mds minor merge has done
  MediumInfoCommon::wait_mds_minor_finish(tablet_id);
  ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());
  ASSERT_EQ(OB_SUCCESS,
            TTLFilterInfoHelper::read_ttl_filter_info_array(
                allocator_,
                LS_ID,
                tablet_id,
                ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION),
                ttl_filter_info_array));

  ASSERT_EQ(2, ttl_filter_info_array.count());
  ASSERT_EQ(OB_SUCCESS, mock_array.at(0)->compare(*ttl_filter_info_array.at(0), equal));
  ASSERT_TRUE(equal);
  ASSERT_EQ(OB_SUCCESS, mock_array.at(2)->compare(*ttl_filter_info_array.at(1), equal));
  ASSERT_TRUE(equal);

  ASSERT_EQ(OB_SUCCESS,
            TTLFilterInfoHelper::read_ttl_filter_info_array(
                allocator_,
                LS_ID,
                tablet_id,
                ObVersionRange(2000, EXIST_READ_SNAPSHOT_VERSION),
                ttl_filter_info_array));

  ASSERT_EQ(1, ttl_filter_info_array.count());
  ASSERT_EQ(OB_SUCCESS, mock_array.at(2)->compare(*ttl_filter_info_array.at(0), equal));
  ASSERT_TRUE(equal);

  // exist new node in mds_sstable
  ttl_filter_info_array.reset();
  collector.reset();
  ASSERT_EQ(OB_SUCCESS,
            tablet_handle.get_obj()->inner_read_info_array_from_mds<ObTTLFilterInfoKey>(
                allocator_, ObVersionRange(100, 200), ttl_filter_info_array, collector));
  ASSERT_TRUE(collector.exist_new_committed_node_);
}

TEST_F(TestTTLFilterInfoReader, test_ttl_filter_info_distinct_mgr)
{
  int ret = OB_SUCCESS;
  bool equal = false;

  const char *key_data = "tx_id    commit_ver  filter_type   filter_value   filter_col\n"
                         "1        1000          1           1              0         \n"
                         "2        2000          1           1000           1         \n"
                         "3        3000          1           2000           0         \n"
                         "4        4000          1           1              1         \n"
                         "5        5000          1           4000           2         \n"
                         "6        6000          1           4000           3         \n"
                         "7        7000          1           4000           2         \n"
                         "8        8000          1           7000           3         \n";

  const ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTTLFilterInfoArray mock_array;
  ObTTLFilterInfoArray ttl_filter_info_array;
  ObMdsReadInfoCollector collector;
  ObTabletHandle tablet_handle;

  ASSERT_EQ(OB_SUCCESS, create_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, TTLFilterInfoHelper::batch_mock_ttl_filter_info_without_sort(allocator_, key_data, mock_array));
  ASSERT_EQ(8, mock_array.count());
  tablet_handle.reset();

  for (int64_t idx = 0; idx < 8; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(idx)));
  }
  ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(7)->commit_version_));
  ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));

  // read all ttl filter info
  ASSERT_EQ(OB_SUCCESS,
            TTLFilterInfoHelper::read_ttl_filter_info_array(
                allocator_,
                LS_ID,
                tablet_id,
                ObVersionRange(1, EXIST_READ_SNAPSHOT_VERSION),
                ttl_filter_info_array));
  ASSERT_EQ(4, ttl_filter_info_array.count());
  ASSERT_EQ(OB_SUCCESS, ttl_filter_info_array.at(0)->compare(*mock_array.at(1), equal));
  ASSERT_TRUE(equal);
  ASSERT_EQ(OB_SUCCESS, ttl_filter_info_array.at(1)->compare(*mock_array.at(2), equal));
  ASSERT_TRUE(equal);
  ASSERT_EQ(OB_SUCCESS, ttl_filter_info_array.at(2)->compare(*mock_array.at(4), equal));
  ASSERT_TRUE(equal);
  ASSERT_EQ(OB_SUCCESS, ttl_filter_info_array.at(3)->compare(*mock_array.at(7), equal));
  ASSERT_TRUE(equal);
}

TEST_F(TestTTLFilterInfoReader, test_ttl_filter_info_cache)
{
  int ret = OB_SUCCESS;

  const char *key_data = "tx_id    commit_ver  filter_type   filter_value   filter_col\n"
                         "1        1000          1           1              0         \n"
                         "2        2000          1           1000           1         \n"
                         "3        3000          1           2000           2         \n"
                         "4        4000          1           3000           3         \n"
                         "5        5000          1           4000           4         \n"
                         "6        -6000         1           1000           5         \n";

  // create tablet
  const ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTTLFilterInfoArray mock_array;
  ObTTLFilterInfoArray ttl_filter_info_array;

  ASSERT_EQ(OB_SUCCESS, create_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, TTLFilterInfoHelper::batch_mock_ttl_filter_info_without_sort(allocator_, key_data, mock_array));
  ASSERT_EQ(6, mock_array.count());
  tablet_handle.reset();

  // insert 5 ttl filter info of same range part
  for (int64_t idx = 0; idx < mock_array.count() - 1; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(idx)));
  }
  ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(4)->commit_version_));
  ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(5)));

  ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());

  ObTTLFilterInfoCache &ttl_filter_info_cache = tablet_handle.get_obj()->ttl_filter_info_cache_;

  // version range not including all ttl filter info
  ASSERT_EQ(OB_SUCCESS,
            tablet_handle.get_obj()->read_ttl_filter_info_array(
                allocator_,
                ObVersionRange(0, mock_array.at(0)->commit_version_ - 1),
                false /*for_access*/,
                ttl_filter_info_array));
  ASSERT_EQ(0, ttl_filter_info_array.count());
  ASSERT_FALSE(ttl_filter_info_cache.is_valid());

  // version range include 2 info
  ttl_filter_info_array.reset();
  ASSERT_EQ(OB_SUCCESS,
            tablet_handle.get_obj()->read_ttl_filter_info_array(
                allocator_,
                ObVersionRange(0, mock_array.at(1)->commit_version_),
                false /*for_access*/,
                ttl_filter_info_array));
  ASSERT_EQ(2, ttl_filter_info_array.count());
  ASSERT_FALSE(ttl_filter_info_cache.is_valid());

  // read all ttl filter info
  ttl_filter_info_array.reset();
  ASSERT_EQ(OB_SUCCESS,
            tablet_handle.get_obj()->read_ttl_filter_info_array(
                allocator_,
                ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION),
                false /*for_access*/,
                ttl_filter_info_array));
  ASSERT_EQ(5, ttl_filter_info_array.count());
  ASSERT_TRUE(ttl_filter_info_cache.is_valid());
  ASSERT_EQ(mock_array.at(4)->commit_version_, ttl_filter_info_cache.newest_commit_version());

  // set one abort info
  ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(5)));

  ttl_filter_info_array.reset();
  ASSERT_EQ(OB_SUCCESS,
            tablet_handle.get_obj()->read_ttl_filter_info_array(
                allocator_,
                ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION),
                false /*for_access*/,
                ttl_filter_info_array));
  ASSERT_EQ(5, ttl_filter_info_array.count());
  ASSERT_TRUE(ttl_filter_info_cache.is_valid());

  ttl_filter_info_array.reset();
  ttl_filter_info_cache.reset();
  ASSERT_EQ(OB_SUCCESS,
            tablet_handle.get_obj()->read_ttl_filter_info_array(
                allocator_, ObVersionRange(0, 30), false /*for_access*/, ttl_filter_info_array));
  ASSERT_TRUE(ttl_filter_info_array.empty());

  ttl_filter_info_array.reset();
  ASSERT_EQ(OB_SUCCESS,
            tablet_handle.get_obj()->read_ttl_filter_info_array(
                allocator_,
                ObVersionRange(mock_array.at(4)->commit_version_, EXIST_READ_SNAPSHOT_VERSION),
                false /*for_access*/,
                ttl_filter_info_array));
  ASSERT_TRUE(ttl_filter_info_array.empty());
}

TEST_F(TestTTLFilterInfoReader, test_put_ttl_filter_info_into_cache)
{
  int ret = OB_SUCCESS;

  const char *key_data = "tx_id    commit_ver  filter_type   filter_value   filter_col\n"
                         "1        1000         1           1              1          \n"
                         "2        2000         1           1000           2          \n"
                         "3        3000         1           2000           3          \n"
                         "4        4000         1           3000           4          \n"
                         "5        5000         1           4000           5          \n"
                         "6        6000         1           5000           6          \n"
                         "7        7000         1           6000           7          \n"
                         "8        8000         1           7000           8          \n";

  // create tablet
  const ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTTLFilterInfoArray mock_array;
  ObTTLFilterInfoArray ttl_filter_info_array;

  ASSERT_EQ(OB_SUCCESS, create_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, TTLFilterInfoHelper::batch_mock_ttl_filter_info_without_sort(allocator_, key_data, mock_array));
  ASSERT_EQ(8, mock_array.count());

#define READ_ARRAY(version_range)                                                                  \
  ASSERT_EQ(OB_SUCCESS,                                                                            \
            TTLFilterInfoHelper::read_ttl_filter_info_array(                                       \
                allocator_, LS_ID, tablet_id, version_range, ttl_filter_info_array));

#define CHECK_RESULT(version_in_array, cache_valid, version_in_cache)                              \
  ASSERT_EQ(version_in_array, ttl_filter_info_array.at(ttl_filter_info_array.count() - 1)->commit_version_); \
  if (cache_valid) {                                                                               \
    ASSERT_TRUE(ttl_filter_info_cache.is_valid());                                                 \
    ASSERT_EQ(ttl_filter_info_cache.newest_commit_version(), version_in_cache);                    \
  }

  {
    for (int64_t idx = 0; idx < 6; ++idx) {
      ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(idx)));
    }
    tablet_handle.reset();
    ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(5)->commit_version_));
    ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
    ASSERT_TRUE(tablet_handle.is_valid());

    ObTTLFilterInfoCache &ttl_filter_info_cache = tablet_handle.get_obj()->ttl_filter_info_cache_;
    READ_ARRAY(ObVersionRange(2000, EXIST_READ_SNAPSHOT_VERSION));
    CHECK_RESULT(6000, false /*cache_valid*/, 0);

    // read all ttl filter info
    READ_ARRAY(ObVersionRange(1, EXIST_READ_SNAPSHOT_VERSION));
    CHECK_RESULT(6000, true /*cache_valid*/, 6000);

    // read ttl filter info with old snapshot (like major)
    READ_ARRAY(ObVersionRange(1, 3000));
    CHECK_RESULT(3000, true /*cache_valid*/, 6000);

    // read ttl filter info with old snapshot (like major)
    READ_ARRAY(ObVersionRange(1, 6000));
    CHECK_RESULT(6000, true /*cache_valid*/, 6000);
  }

  {
    for (int64_t idx = 6; idx < 8; ++idx) {
      ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(idx)));
    }
    tablet_handle.reset();
    ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(7)->commit_version_));
    ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
    ASSERT_TRUE(tablet_handle.is_valid());

    ObTTLFilterInfoCache &ttl_filter_info_cache = tablet_handle.get_obj()->ttl_filter_info_cache_;
    // cache is still valid becase we have copy mds cache from old tablet to new tablet
    // but this case directly call mds->log_commit, so the cache is not set invalid
    ASSERT_TRUE(ttl_filter_info_cache.is_valid());

    // replay ttl filter info should make the cache invalid, we reset cache manually
    ttl_filter_info_cache.reset();
    ASSERT_FALSE(ttl_filter_info_cache.is_valid());

    READ_ARRAY(ObVersionRange(2000, 7000));
    CHECK_RESULT(7000, false /*cache_valid*/, 0);

    READ_ARRAY(ObVersionRange(1, 7000));
    CHECK_RESULT(7000, false /*cache_valid*/, 0);

    READ_ARRAY(ObVersionRange(1, 8000));
    CHECK_RESULT(8000, true /*cache_valid*/, 8000);
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ttl_filter_info_reader.log*");
  system("rm -f test_ttl_filter_info_reader_rs.log*");
  system("rm -f test_ttl_filter_info_reader_election.log*");
  OB_LOGGER.set_file_name("test_ttl_filter_info_reader.log",
                          true,
                          true,
                          "test_ttl_filter_info_reader_rs.log",        // RS 日志文件
                          "test_ttl_filter_info_reader_election.log"); // ELEC 日志文件
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
