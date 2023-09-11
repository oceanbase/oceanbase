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

#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "share/config/ob_server_config.h"
#include "unittest/storage/init_basic_struct.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"
#include "src/storage/compaction/ob_medium_list_checker.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;
using namespace oceanbase::compaction;

#define USING_LOG_PREFIX STORAGE

#define APPEND_MEDIUM_INFO(dumped_medium_info, allocator, medium_snapshot, last_medium_snapshot, from_cur_cluster, info) \
    ret = create_medium_info(allocator, medium_snapshot, last_medium_snapshot, from_cur_cluster, info); \
    ASSERT_EQ(OB_SUCCESS, ret); \
    ret = dumped_medium_info.append(*info); \
    ASSERT_EQ(OB_SUCCESS, ret);

namespace oceanbase
{
namespace unittest
{
class TestTabletDumpedMediumInfo : public ::testing::Test
{
public:
  TestTabletDumpedMediumInfo() = default;
  virtual ~TestTabletDumpedMediumInfo() = default;
public:
  static int create_medium_info(
      common::ObIAllocator &allocator,
      const int64_t medium_snapshot,
      const int64_t last_medium_snapshot,
      const bool from_cur_cluster,
      compaction::ObMediumCompactionInfo *&info);
};

int TestTabletDumpedMediumInfo::create_medium_info(
    common::ObIAllocator &allocator,
    const int64_t medium_snapshot,
    const int64_t last_medium_snapshot,
    const bool from_cur_cluster,
    compaction::ObMediumCompactionInfo *&info)
{
  int ret = OB_SUCCESS;
  constexpr int64_t size = sizeof(compaction::ObMediumCompactionInfo);
  void *buffer = allocator.alloc(size);
  if (OB_ISNULL(buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(size));
  } else {
    info = new (buffer) compaction::ObMediumCompactionInfo();

    info->compaction_type_ = compaction::ObMediumCompactionInfo::ObCompactionType::MEDIUM_COMPACTION;
    info->medium_snapshot_ = medium_snapshot;
    info->last_medium_snapshot_ = last_medium_snapshot;
    info->data_version_ = 100;
    info->cluster_id_ = from_cur_cluster ? GCONF.cluster_id : 9527;

    // storage schema
    const uint64_t table_id = 1234567;
    share::schema::ObTableSchema table_schema;
    build_test_schema(table_schema, table_id);
    ret = info->storage_schema_.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL);
  }

  return ret;
}

TEST_F(TestTabletDumpedMediumInfo, overlap)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  compaction::ObMediumCompactionInfo *info = nullptr;

  ObTabletDumpedMediumInfo input_medium_info1;
  ret = input_medium_info1.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 5, 4, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 6, 5, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 7, 6, true, info);

  ObTabletDumpedMediumInfo input_medium_info2;
  ret = input_medium_info2.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 2, 1, true, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 3, 2, true, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 4, 3, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 5, 4, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 6, 5, true, info);

  ObTabletDumpedMediumInfo result;
  ret = result.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t finish_medium_scn = 3;
  ret = ObTabletMdsData::copy_medium_info_list(finish_medium_scn, input_medium_info1, input_medium_info2, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, result.medium_info_list_.count());
  ASSERT_EQ(4, result.medium_info_list_.at(0)->medium_snapshot_);
  ASSERT_EQ(5, result.medium_info_list_.at(1)->medium_snapshot_);
  ASSERT_EQ(6, result.medium_info_list_.at(2)->medium_snapshot_);
  ASSERT_EQ(7, result.medium_info_list_.at(3)->medium_snapshot_);

  ret = ObMediumListChecker::check_continue(result.medium_info_list_);

  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTabletDumpedMediumInfo, no_overlap)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  compaction::ObMediumCompactionInfo *info = nullptr;

  ObTabletDumpedMediumInfo input_medium_info1;
  ret = input_medium_info1.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 5, 4, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 6, 5, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 7, 6, true, info);

  ObTabletDumpedMediumInfo input_medium_info2;
  ret = input_medium_info2.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 2, 1, true, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 3, 2, true, info);

  {
    ObTabletDumpedMediumInfo result;
    ret = result.init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);

    const int64_t finish_medium_scn = 1;
    ret = ObTabletMdsData::copy_medium_info_list(finish_medium_scn, input_medium_info1, input_medium_info2, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(5, result.medium_info_list_.count());
    ASSERT_EQ(2, result.medium_info_list_.at(0)->medium_snapshot_);
    ASSERT_EQ(3, result.medium_info_list_.at(1)->medium_snapshot_);
    ASSERT_EQ(5, result.medium_info_list_.at(2)->medium_snapshot_);
    ASSERT_EQ(6, result.medium_info_list_.at(3)->medium_snapshot_);
    ASSERT_EQ(7, result.medium_info_list_.at(4)->medium_snapshot_);

    ret = ObMediumListChecker::check_continue(result.medium_info_list_);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  }

  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 4, 3, true, info);
  {
    ObTabletDumpedMediumInfo result;
    ret = result.init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);

    const int64_t finish_medium_scn = 1;
    ret = ObTabletMdsData::copy_medium_info_list(finish_medium_scn, input_medium_info1, input_medium_info2, result);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(6, result.medium_info_list_.count());
    ASSERT_EQ(2, result.medium_info_list_.at(0)->medium_snapshot_);
    ASSERT_EQ(3, result.medium_info_list_.at(1)->medium_snapshot_);
    ASSERT_EQ(4, result.medium_info_list_.at(2)->medium_snapshot_);
    ASSERT_EQ(5, result.medium_info_list_.at(3)->medium_snapshot_);
    ASSERT_EQ(6, result.medium_info_list_.at(4)->medium_snapshot_);
    ASSERT_EQ(7, result.medium_info_list_.at(5)->medium_snapshot_);

    ret = ObMediumListChecker::check_continue(result.medium_info_list_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestTabletDumpedMediumInfo, overlap_and_filter)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  compaction::ObMediumCompactionInfo *info = nullptr;

  ObTabletDumpedMediumInfo input_medium_info1;
  ret = input_medium_info1.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 5, 4, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 6, 5, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 7, 6, true, info);

  ObTabletDumpedMediumInfo input_medium_info2;
  ret = input_medium_info2.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 2, 1, true, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 3, 2, true, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 4, 3, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 5, 4, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 6, 5, true, info);

  ObTabletDumpedMediumInfo result;
  ret = result.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t finish_medium_scn = 6;
  ret = ObTabletMdsData::copy_medium_info_list(finish_medium_scn, input_medium_info1, input_medium_info2, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, result.medium_info_list_.count());
  ASSERT_EQ(7, result.medium_info_list_.at(0)->medium_snapshot_);

  ret = ObMediumListChecker::check_continue(result.medium_info_list_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTabletDumpedMediumInfo, no_overlap_and_filter)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  compaction::ObMediumCompactionInfo *info = nullptr;

  ObTabletDumpedMediumInfo input_medium_info1;
  ret = input_medium_info1.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 5, 4, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 6, 5, true, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 7, 6, true, info);

  ObTabletDumpedMediumInfo input_medium_info2;
  ret = input_medium_info2.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 2, 1, true, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 3, 2, true, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 4, 3, true, info);

  ObTabletDumpedMediumInfo result;
  ret = result.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t finish_medium_scn = 7;
  ret = ObTabletMdsData::copy_medium_info_list(finish_medium_scn, input_medium_info1, input_medium_info2, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, result.medium_info_list_.count());

  ret = ObMediumListChecker::check_continue(result.medium_info_list_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTabletDumpedMediumInfo, standby_cluster)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  compaction::ObMediumCompactionInfo *info = nullptr;

  ObTabletDumpedMediumInfo input_medium_info1;
  ret = input_medium_info1.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 7, 5, false, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 9, 8, false, info);

  ObTabletDumpedMediumInfo input_medium_info2;
  ret = input_medium_info2.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 2, 1, false, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 4, 2, false, info);

  ObTabletDumpedMediumInfo result;
  ret = result.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t finish_medium_scn = 3;
  ret = ObTabletMdsData::copy_medium_info_list(finish_medium_scn, input_medium_info1, input_medium_info2, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.medium_info_list_.count());
  ASSERT_EQ(4, result.medium_info_list_.at(0)->medium_snapshot_);
  ASSERT_EQ(7, result.medium_info_list_.at(1)->medium_snapshot_);
  ASSERT_EQ(9, result.medium_info_list_.at(2)->medium_snapshot_);

  ret = ObMediumListChecker::check_continue(result.medium_info_list_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTabletDumpedMediumInfo, mds_table_dump)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  compaction::ObMediumCompactionInfo *info = nullptr;

  ObTabletDumpedMediumInfo input_medium_info1;
  ret = input_medium_info1.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletComplexAddr<ObTabletDumpedMediumInfo> mds_table_data;
  mds_table_data.ptr_ = &input_medium_info1;

  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 7, 6, false, info);
  APPEND_MEDIUM_INFO(input_medium_info1, allocator, 9, 7, false, info);

  ObTabletDumpedMediumInfo input_medium_info2;
  ret = input_medium_info2.init_for_first_creation(allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletComplexAddr<ObTabletDumpedMediumInfo> base_data;
  base_data.ptr_ = &input_medium_info2;

  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 2, 1, false, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 4, 2, false, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 6, 4, false, info);
  APPEND_MEDIUM_INFO(input_medium_info2, allocator, 7, 6, false, info);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletComplexAddr<ObTabletDumpedMediumInfo> result_data;
  ObTabletDumpedMediumInfo *&result = result_data.ptr_;

  const int64_t finish_medium_scn = 4;
  ret = ObTabletMdsData::init_single_complex_addr(allocator, mds_table_data, base_data, finish_medium_scn, result_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result->medium_info_list_.count());
  ASSERT_EQ(6, result->medium_info_list_.at(0)->medium_snapshot_);
  ASSERT_EQ(7, result->medium_info_list_.at(1)->medium_snapshot_);
  ASSERT_EQ(9, result->medium_info_list_.at(2)->medium_snapshot_);
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tablet_dumped_medium_info.log*");
  OB_LOGGER.set_file_name("test_tablet_dumped_medium_info.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
