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
#include <gtest/gtest.h>
#define private public
#define protected public
#include "mtlenv/storage/medium_info_common.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;

namespace oceanbase
{
using namespace compaction;
namespace storage
{
class TestTruncateInfoReader : public MediumInfoCommon
{
public:
  TestTruncateInfoReader() = default;
  virtual ~TestTruncateInfoReader() = default;
};

TEST_F(TestTruncateInfoReader, read_multi_truncate_info_from_minor)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  bool equal = false;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet_handle.reset();

  const char *key_data =
    "tx_id    commit_ver    schema_ver    lower_bound    upper_bound\n"
    "1        1000         10100         100            200        \n"
    "3        3000         10300         200            300        \n";
  const char *key_data2 =
    "tx_id    commit_ver    schema_ver    lower_bound    upper_bound\n"
    "2        -2000        10200         100            200        \n";
  ObTruncateInfoArray mock_array;
  ObTruncateInfoArray mock_array2;
  ObTruncateInfoArray truncate_info_array;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::batch_mock_truncate_info(allocator_, key_data, mock_array));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::batch_mock_truncate_info(allocator_, key_data2, mock_array2));
  ASSERT_EQ(2, mock_array.count());
  ASSERT_EQ(1, mock_array2.count());
  int64_t idx = 0;
  {
    // insert data into mds table
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array.at(idx)));
    LOG_INFO("insert truncate info", KR(ret), K(idx), KPC(mock_array.at(idx)));
    // exist new node in mds_table
    truncate_info_array.reset();
    ObMdsReadInfoCollector collector;
    ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->inner_read_truncate_info_array_from_mds(allocator_, ObVersionRange(100, 200), truncate_info_array, collector));
    ASSERT_TRUE(collector.exist_new_committed_node_);
    tablet_handle.reset();

    ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(idx)->commit_version_));
    ++idx;
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array2.at(0)));
    LOG_INFO("insert truncate info", KR(ret), K(idx), KPC(mock_array2.at(0)));
  }
  {
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array.at(idx)));
    LOG_INFO("insert truncate info", KR(ret), K(idx), KPC(mock_array.at(idx)));
    ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(idx)->commit_version_));
  }

  // check mds minor merge has done
  MediumInfoCommon::wait_mds_minor_finish(tablet_id);
  ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (tablet_handle.is_valid()) {
    ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, LS_ID, tablet_id,
                  ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));

    ASSERT_EQ(2, truncate_info_array.count());
  #define COMPARE(idx, array_index) \
    ASSERT_EQ(OB_SUCCESS, mock_array.at(idx)->compare(*truncate_info_array.at(array_index), equal)); \
    ASSERT_TRUE(equal);
    COMPARE(0, 0);
    COMPARE(1, 1);

    // read with version_range, will hit kv cache, will return 2 row
    ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, LS_ID, tablet_id,
                  ObVersionRange(2000, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));

    ASSERT_EQ(1, truncate_info_array.count());
    COMPARE(1, 0);

    LOG_INFO("read from mds sstable", KR(ret));
    // exist new node in mds_sstable
    truncate_info_array.reset();
    ObMdsReadInfoCollector collector;
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->inner_read_truncate_info_array_from_mds(allocator_, ObVersionRange(100, 200), truncate_info_array, collector));
    ASSERT_TRUE(collector.exist_new_committed_node_);
  }
}

TEST_F(TestTruncateInfoReader, test_truncate_info_distinct_mgr)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();

  /*
  * insert 5 truncate info of same range part
  */
  ObTruncateInfo info;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1/*part_key_idx*/, info.truncate_part_));
  int64_t trans_id = 100;
  int64_t commit_version = 1000;
  int64_t schema_version = 10100;
  for (int64_t idx = 0; idx < 5; ++idx) {
    TruncateInfoHelper::mock_truncate_info(allocator_, trans_id + idx, schema_version + idx, info);
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(trans_id + idx, commit_version + idx, info));
  } // for

  { // last truncate info have largest commit version, use last truncate info to compare
    ObTruncateInfoArray distinct_array;
    distinct_array.init_for_first_creation(allocator_);
    ObMdsInfoDistinctMgr mgr;
    ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObVersionRange read_version_range(0, EXIST_READ_SNAPSHOT_VERSION);
    if (tablet_handle.is_valid()) {
      ASSERT_EQ(OB_SUCCESS, mgr.init(allocator_, *tablet_handle.get_obj(), nullptr/*split_extra_tablet_handles*/, read_version_range, false/*access*/));
      ASSERT_EQ(OB_SUCCESS, mgr.get_distinct_truncate_info_array(distinct_array));
      ASSERT_EQ(1, distinct_array.count());
      bool equal = 0;
      COMMON_LOG(INFO, "compare", KR(ret), KPC(distinct_array.get_array().at(0)), K(info));
      ASSERT_EQ(OB_SUCCESS, info.compare(*distinct_array.get_array().at(0), equal)); // key=104, commit_version=1004
      ASSERT_EQ(equal, true);
    }
  }

  /*
  * insert 5 truncate info of same list part
  */
  info.destroy();
  trans_id = 200;
  const int64_t list_val_cnt = 3;
  int64_t list_vals[] = {200, 300, 100};
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, list_vals, list_val_cnt, info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1/*part_key_idx*/, info.truncate_part_));
  for (int64_t idx = 5; idx >= 0; --idx) {
    TruncateInfoHelper::mock_truncate_info(allocator_, trans_id + idx, 2 * schema_version - idx, info);
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(trans_id + idx, 2 * commit_version - idx, info));
  } // for
  { // last truncate info have largest commit version, use last truncate info to compare
    ObTruncateInfoArray distinct_array;
    distinct_array.init_for_first_creation(allocator_);
    ObMdsInfoDistinctMgr mgr;
    ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObVersionRange read_version_range(0, EXIST_READ_SNAPSHOT_VERSION);
    if (tablet_handle.is_valid()) {
      tablet_handle.get_obj()->truncate_info_cache_.reset();
      ASSERT_EQ(OB_SUCCESS, mgr.init(allocator_, *tablet_handle.get_obj(), nullptr/*split_extra_tablet_handles*/, read_version_range, false/*access*/));
      ASSERT_EQ(OB_SUCCESS, mgr.get_distinct_truncate_info_array(distinct_array));
      COMMON_LOG(INFO, "print", K(distinct_array));
      ASSERT_EQ(2, distinct_array.count());
      // first truncate info is range part
      bool equal = 0;
      // [0] key=104, commit_version=1004
      // [1] key=200, commit_version=2000
      ASSERT_EQ(OB_SUCCESS, info.compare(*distinct_array.get_array().at(1), equal));
      ASSERT_EQ(equal, true);

      ObTruncateInfoArray cached_array;
      ObTruncateInfoCacheKey cache_key(MTL_ID(), tablet_id, distinct_array.get_array().at(1)->schema_version_, tablet_handle.get_obj()->get_last_major_snapshot_version());
      ASSERT_EQ(OB_SUCCESS, ObTruncateInfoKVCacheUtil::get_truncate_info_array(allocator_, cache_key, cached_array));
      COMMON_LOG(INFO, "print", K(cached_array));
      ASSERT_EQ(2, cached_array.count());
    }

    // test ObMdsFilterInfo serialization
    const int64_t BUF_LEN = 10 * 1024;
    ObArenaAllocator str_alloctor;
    char *buf = (char *)str_alloctor.alloc(sizeof(char) * BUF_LEN);
    ASSERT_TRUE(nullptr != buf);

    ObMdsFilterInfo mds_filter_info;
    int64_t write_pos = 0;
    ASSERT_EQ(OB_ERR_UNEXPECTED, mds_filter_info.serialize(buf, BUF_LEN, write_pos));

    ASSERT_EQ(OB_SUCCESS, mgr.fill_mds_filter_info(allocator_, mds_filter_info));
    ASSERT_EQ(OB_SUCCESS, mds_filter_info.serialize(buf, BUF_LEN, write_pos));
    ASSERT_EQ(mds_filter_info.get_serialize_size(), write_pos);

    ObMdsFilterInfo tmp_filter_info;
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, tmp_filter_info.deserialize(allocator_, buf, write_pos, read_pos));
    ASSERT_EQ(2, tmp_filter_info.truncate_info_keys_.cnt_);
    COMMON_LOG(INFO, "print", K(tmp_filter_info));
    mds_filter_info.destroy(allocator_);
    tmp_filter_info.destroy(allocator_);
  }
}

TEST_F(TestTruncateInfoReader, test_truncate_info_distinct_mgr_with_subpart)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();

  /*
  * insert 5 truncate info of same range part
  */
  ObTruncateInfo info;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1/*part_key_idx*/, info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 30, 40, info.truncate_subpart_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1/*part_key_idx*/, info.truncate_subpart_));
  info.is_sub_part_ = true;

  int64_t trans_id = 100;
  int64_t commit_version = 1000;
  int64_t schema_version = 10100;
  for (int64_t idx = 0; idx < 5; ++idx) {
    TruncateInfoHelper::mock_truncate_info(allocator_, trans_id + idx, schema_version + idx, info);
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(trans_id + idx, commit_version + idx, info));
  } // for

  { // last truncate info have largest commit version, use last truncate info to compare
    ObTruncateInfoArray distinct_array;
    distinct_array.init_for_first_creation(allocator_);
    ObMdsInfoDistinctMgr mgr;
    ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObVersionRange read_version_range(0, EXIST_READ_SNAPSHOT_VERSION);
    if (tablet_handle.is_valid()) {
      tablet_handle.get_obj()->truncate_info_cache_.reset();
      ASSERT_EQ(OB_SUCCESS, mgr.init(allocator_, *tablet_handle.get_obj(), nullptr/*split_extra_tablet_handles*/, read_version_range, false/*access*/));
      ASSERT_EQ(OB_SUCCESS, mgr.get_distinct_truncate_info_array(distinct_array));
      ASSERT_EQ(1, distinct_array.count());
      bool equal = 0;
      COMMON_LOG(INFO, "compare", KR(ret), KPC(distinct_array.get_array().at(0)), K(info));
      ASSERT_EQ(OB_SUCCESS, info.compare(*distinct_array.get_array().at(0), equal));
      ASSERT_EQ(equal, true);
    }
  }
   // mock another truncate info without subpart
  info.is_sub_part_ = false;
  info.schema_version_ += 100;
  ASSERT_EQ(OB_SUCCESS, insert_truncate_info(trans_id + 100, commit_version + 100, info));
  {
    ObTruncateInfoArray distinct_array;
    distinct_array.init_for_first_creation(allocator_);
    ObMdsInfoDistinctMgr mgr;
    ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObVersionRange read_version_range(0, EXIST_READ_SNAPSHOT_VERSION);
    if (tablet_handle.is_valid()) {
      tablet_handle.get_obj()->truncate_info_cache_.reset();
      ASSERT_EQ(OB_SNAPSHOT_DISCARDED, mgr.init(allocator_, *tablet_handle.get_obj(), nullptr/*split_extra_tablet_handles*/, ObVersionRange(0, 5), true/*access*/));
      tablet_handle.get_obj()->truncate_info_cache_.reset();
      ASSERT_EQ(OB_SUCCESS, mgr.init(allocator_, *tablet_handle.get_obj(), nullptr/*split_extra_tablet_handles*/, read_version_range, false/*access*/));
      ASSERT_EQ(OB_SUCCESS, mgr.get_distinct_truncate_info_array(distinct_array));
      ASSERT_EQ(2, distinct_array.count());
      bool equal = 0;
      COMMON_LOG(INFO, "compare", KR(ret), KPC(distinct_array.get_array().at(1)), K(info));
      ASSERT_EQ(OB_SUCCESS, info.compare(*distinct_array.get_array().at(1), equal));
      ASSERT_EQ(equal, true);
    }
  }
}

TEST_F(TestTruncateInfoReader, test_truncate_info_cache)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet_handle.reset();
  const char *key_data =
    "tx_id    commit_ver    schema_ver    lower_bound    upper_bound\n"
    "1        1000         10100         100            200        \n"
    "200      2000         10200         100            200        \n"
    "3        3000         10300         100            200        \n"
    "40       4000         10400         100            200        \n"
    "5        5000         10500         100            200        \n";
  const char *key_data2 =
    "tx_id    commit_ver    schema_ver    lower_bound    upper_bound\n"
    "6        -6000        10600         100            200        \n";
  ObTruncateInfoArray mock_array;
  ObTruncateInfoArray mock_array2;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::batch_mock_truncate_info(allocator_, key_data, mock_array));
  ASSERT_EQ(5, mock_array.count());
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::batch_mock_truncate_info(allocator_, key_data2, mock_array2));
  ASSERT_EQ(1, mock_array2.count());
  /*
  * insert 5 truncate info of same range part
  */

  for (int64_t idx = 0; idx < mock_array.count(); ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array.at(idx)));
  } // for
  LOG_INFO("print truncate info array", KR(ret), K(mock_array));
  ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(mock_array.count() - 1)->commit_version_));

  for (int64_t idx = 0; idx < mock_array2.count(); ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array2.at(idx)));
  } // for
  ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTruncateInfoArray truncate_info_array;
  ASSERT_EQ(OB_SUCCESS, ret);
  if (tablet_handle.is_valid()) {
    ObTruncateInfoCache &truncate_info_cache = tablet_handle.get_obj()->truncate_info_cache_;
    /*
    * version range not including all truncate info
    */
    ret = tablet_handle.get_obj()->read_truncate_info_array(allocator_, ObVersionRange(0, mock_array.at(0)->commit_version_ - 1), false/*for_access*/, truncate_info_array);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, truncate_info_array.count());
    ASSERT_FALSE(truncate_info_cache.is_valid());

    /*
    * version range include 2 info
    */
    truncate_info_array.reset();
    ret = tablet_handle.get_obj()->read_truncate_info_array(allocator_, ObVersionRange(0, mock_array.at(1)->commit_version_), false/*for_access*/, truncate_info_array);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, truncate_info_array.count());
    ASSERT_FALSE(truncate_info_cache.is_valid());

    /*
    * read all truncate info
    */
    truncate_info_array.reset();
    ret = tablet_handle.get_obj()->read_truncate_info_array(allocator_, ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), false/*for_access*/, truncate_info_array);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(5, truncate_info_array.count());
    ASSERT_TRUE(truncate_info_cache.is_valid());
    ASSERT_EQ(mock_array.at(mock_array.count() - 1)->commit_version_, truncate_info_cache.newest_commit_version_);

    truncate_info_cache.reset();
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array2.at(0))); // set one abort info

    truncate_info_array.reset();
    ret = tablet_handle.get_obj()->read_truncate_info_array(allocator_, ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), false/*for_access*/, truncate_info_array);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(5, truncate_info_array.count());
    ASSERT_TRUE(truncate_info_cache.is_valid());

    truncate_info_array.reset();
    tablet_handle.get_obj()->truncate_info_cache_.reset();
    ret = tablet_handle.get_obj()->read_truncate_info_array(allocator_, ObVersionRange(0, 30), false/*for_access*/, truncate_info_array);
    ASSERT_TRUE(truncate_info_array.empty());

    truncate_info_array.reset();
    ret = tablet_handle.get_obj()->read_truncate_info_array(allocator_, ObVersionRange(mock_array.at(mock_array.count() - 1)->commit_version_, EXIST_READ_SNAPSHOT_VERSION), false/*for_access*/, truncate_info_array);
    ASSERT_TRUE(truncate_info_array.empty());
  }
}

TEST_F(TestTruncateInfoReader, test_put_truncate_info_into_cache)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet_handle.reset();
  const char *key_data =
    "tx_id    commit_ver    schema_ver    lower_bound    upper_bound\n"
    "100      1000         10100         100            200        \n"
    "2        2000         10200         100            200        \n"
    "30       3000         10300         100            200        \n"
    "4        4000         10400         100            200        \n"
    "50       5000         10500         100            200        \n"
    "60       6000         10600         100            200        \n"
    "7        7000         10700         100            200        \n"
    "8        8000         10800         100            200        \n";
  ObTruncateInfoArray mock_array;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::batch_mock_truncate_info(allocator_, key_data, mock_array));
  const int64_t max_cnt = 8;
  const int64_t first_round = 6;
  int64_t idx = 0;
  ASSERT_EQ(max_cnt, mock_array.count());
  /*
  * insert 5 truncate info of same range part
  */
  for (; idx < first_round; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array.at(idx)));
  } // for
  ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(first_round - 1)->commit_version_));
  ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTruncateInfoArray truncate_info_array;
#define READ_ARRAY(version_range)                                              \
  TruncateInfoHelper::read_distinct_truncate_info_array(                       \
      allocator_, LS_ID, tablet_id, version_range,                             \
      truncate_info_array);                                                    \
  ASSERT_EQ(OB_SUCCESS, ret);
#define CHECK_RESULT(version_in_array, cache_valid, version_in_cache)          \
  ASSERT_EQ(1, truncate_info_array.count());                                   \
  ASSERT_EQ(version_in_array, truncate_info_array.at(0)->commit_version_);     \
  if (cache_valid) {                                                           \
    ASSERT_TRUE(truncate_info_cache.is_valid());                               \
    ASSERT_EQ(truncate_info_cache.newest_commit_version(), version_in_cache);  \
  }
  if (tablet_handle.is_valid()) {
    ObTruncateInfoCache &truncate_info_cache = tablet_handle.get_obj()->truncate_info_cache_;
    READ_ARRAY(ObVersionRange(2000, EXIST_READ_SNAPSHOT_VERSION));
    CHECK_RESULT(6000, false/*cache_valid*/, 0);
    /*
    * read all truncate info
    */
    READ_ARRAY(ObVersionRange(1, EXIST_READ_SNAPSHOT_VERSION));
    CHECK_RESULT(6000, true/*cache_valid*/, 6000);
    /*
    * read truncate info with old snapshot (like major)
    */
    READ_ARRAY(ObVersionRange(1, 3000));
    CHECK_RESULT(3000, true/*cache_valid*/, 6000);
    /*
    * read truncate info with old snapshot (like major)
    */
    READ_ARRAY(ObVersionRange(1, 6000));
    CHECK_RESULT(6000, true/*cache_valid*/, 6000);
  }
  for (; idx < max_cnt; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array.at(idx)));
  } // for
  tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(max_cnt - 1)->commit_version_));
  ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (tablet_handle.is_valid()) {
    ObTruncateInfoCache &truncate_info_cache = tablet_handle.get_obj()->truncate_info_cache_;
    ASSERT_FALSE(truncate_info_cache.is_valid());

    READ_ARRAY(ObVersionRange(2000, 7000));
    CHECK_RESULT(7000, false/*cache_valid*/, 0);

    READ_ARRAY(ObVersionRange(1, 7000));
    CHECK_RESULT(7000, false/*cache_valid*/, 0);

    READ_ARRAY(ObVersionRange(1, 8000));
    CHECK_RESULT(8000, true/*cache_valid*/, 8000);
  }
}

TEST_F(TestTruncateInfoReader, test_random_key_with_same_part_def)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet_handle.reset();
  const char *key_data =
    "tx_id    commit_ver    schema_ver    lower_bound    upper_bound\n"
    "100      1000         10100         100            200        \n"
    "2        2000         10200         200            300        \n"
    "30       3000         10300         100            200        \n"
    "4        4000         10400         200            300        \n";
  ObTruncateInfoArray mock_array;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::batch_mock_truncate_info(allocator_, key_data, mock_array));
  const int64_t max_cnt = 4;
  int64_t idx = 0;
  ASSERT_EQ(max_cnt, mock_array.count());
  /*
  * insert 5 truncate info of same range part
  */
  for (; idx < max_cnt; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array.at(idx)));
  } // for
  ASSERT_EQ(OB_SUCCESS, wait_mds_mini_finish(tablet_id, mock_array.at(max_cnt - 1)->commit_version_));
  ret = MediumInfoCommon::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObMdsInfoDistinctMgr distinct_mgr;
#define READ_DISTINC_ARRAY(version_range)                                              \
  distinct_mgr.reset(); \
  ASSERT_EQ(OB_SUCCESS, distinct_mgr.init(allocator_, *tablet_handle.get_obj(), nullptr, version_range, false/*access*/));
  // read all truncate info, should get 2 distinct info
  READ_DISTINC_ARRAY(ObVersionRange(1, EXIST_READ_SNAPSHOT_VERSION));
  LOG_INFO("read all truncate info", KR(ret), K(distinct_mgr));
  ASSERT_EQ(2, distinct_mgr.get_distinct_truncate_info_array().count());
  ASSERT_EQ(mock_array.at(2)->commit_version_, distinct_mgr.get_distinct_truncate_info_array().at(0)->commit_version_);
  ASSERT_EQ(mock_array.at(3)->commit_version_, distinct_mgr.get_distinct_truncate_info_array().at(1)->commit_version_);

  if (tablet_handle.is_valid()) {
    ObTruncateInfoCache &truncate_info_cache = tablet_handle.get_obj()->truncate_info_cache_;
    truncate_info_cache.reset();
    READ_DISTINC_ARRAY(ObVersionRange(1000, EXIST_READ_SNAPSHOT_VERSION));
    LOG_INFO("read all truncate info", KR(ret), K(distinct_mgr));
    ASSERT_EQ(2, distinct_mgr.get_distinct_truncate_info_array().count());
    ASSERT_EQ(mock_array.at(2)->commit_version_, distinct_mgr.get_distinct_truncate_info_array().at(0)->commit_version_);
    ASSERT_EQ(mock_array.at(3)->commit_version_, distinct_mgr.get_distinct_truncate_info_array().at(1)->commit_version_);
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_truncate_info_reader.log*");
  OB_LOGGER.set_file_name("test_truncate_info_reader.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
