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
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/tablet/ob_mds_range_query_iterator.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{
class TestMediumInfoIterator : public MediumInfoCommon
{
public:
  TestMediumInfoIterator() = default;
  virtual ~TestMediumInfoIterator() = default;
};

TEST_F(TestMediumInfoIterator, pure_mds_table)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 5);
    user_data.create_commit_scn_ = commit_scn;
    user_data.create_commit_version_ = 5;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(5)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(commit_scn, commit_scn);
  }

  // insert data into mds table
  ret = insert_medium_info(10, 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(20, 20);
  ASSERT_EQ(OB_SUCCESS, ret);

  // query
  common::ObArenaAllocator arena_allocator;
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;
  ObTableScanParam scan_param;
  share::SCN read_snapshot = share::SCN::plus(share::SCN::min_scn(), 30);

  ret = ObMdsScanParamHelper::build_scan_param(
      arena_allocator,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      common::ObString()/*udf_key*/,
      false/*is_get*/,
      ObClockGenerator::getClock() + 1_s,
      read_snapshot,
      scan_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreCtx store_ctx;
  ObMdsRangeQueryIterator<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> range_query_iter;
  ret = tablet->mds_range_query<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      scan_param, store_ctx, range_query_iter);
  ASSERT_EQ(OB_SUCCESS, ret);

  mds::MdsDumpKV *kv = nullptr;
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(10, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(20, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

TEST_F(TestMediumInfoIterator, pure_mds_sstable)
{
  int ret = OB_SUCCESS;

  // get ls
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();

  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 5);
    user_data.create_commit_scn_ = commit_scn;
    user_data.create_commit_version_ = 5;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(5)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(commit_scn, commit_scn);
  }

  // insert data into mds table
  ret = insert_medium_info(10, 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(20, 20);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(30, 30);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mds table flush
  share::SCN decided_scn = share::SCN::plus(share::SCN::min_scn(), 50);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  tablet_handle.reset();
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // query
  ret = ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // query
  common::ObArenaAllocator arena_allocator;
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;
  ObTableScanParam scan_param;
  share::SCN read_snapshot = share::SCN::plus(share::SCN::min_scn(), 22);

  ret = ObMdsScanParamHelper::build_scan_param(
      arena_allocator,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      common::ObString()/*udf_key*/,
      false/*is_get*/,
      ObClockGenerator::getClock() + 1_s,
      read_snapshot,
      scan_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreCtx store_ctx;
  ObMdsRangeQueryIterator<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> range_query_iter;
  ret = tablet->mds_range_query<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      scan_param, store_ctx, range_query_iter);
  ASSERT_EQ(OB_SUCCESS, ret);

  mds::MdsDumpKV *kv = nullptr;
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(10, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(20, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

TEST_F(TestMediumInfoIterator, data_overlap)
{

}

TEST_F(TestMediumInfoIterator, data_no_overlap)
{
  int ret = OB_SUCCESS;

  // get ls
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();

  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 5);
    user_data.create_commit_scn_ = commit_scn;
    user_data.create_commit_version_ = 5;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(5)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(commit_scn, commit_scn);
  }

  // insert data into mds table
  ret = insert_medium_info(100, 100);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(200, 200);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(300, 300);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mds table flush
  share::SCN decided_scn = share::SCN::plus(share::SCN::min_scn(), 310);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  tablet_handle.reset();
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // insert data into mds table
  ret = insert_medium_info(400, 400);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(500, 500);
  ASSERT_EQ(OB_SUCCESS, ret);

  // query
  ret = ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  common::ObArenaAllocator arena_allocator;
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;
  ObTableScanParam scan_param;
  share::SCN read_snapshot = share::SCN::plus(share::SCN::min_scn(), 444);
  ret = ObMdsScanParamHelper::build_scan_param(
      arena_allocator,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      common::ObString()/*udf_key*/,
      false/*is_get*/,
      ObClockGenerator::getClock() + 1_s,
      read_snapshot,
      scan_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreCtx store_ctx;
  ObMdsRangeQueryIterator<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> range_query_iter;
  ret = tablet->mds_range_query<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      scan_param, store_ctx, range_query_iter);
  ASSERT_EQ(OB_SUCCESS, ret);

  mds::MdsDumpKV *kv = nullptr;
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(100, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(200, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(300, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(400, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

TEST_F(TestMediumInfoIterator, full_inclusion)
{
  int ret = OB_SUCCESS;

  // get ls
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();

  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 5);
    user_data.create_commit_scn_ = commit_scn;
    user_data.create_commit_version_ = 5;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(5)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(commit_scn, commit_scn);
  }

  // insert data into mds table
  ret = insert_medium_info(10, 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(20, 20);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(30, 30);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mds table flush
  share::SCN decided_scn = share::SCN::plus(share::SCN::min_scn(), 31);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // nodes are not released
  int64_t node_cnt = INT64_MAX;

  ::ob_usleep(6_s);
  ret = mds_table_.get_node_cnt(node_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, node_cnt);

  // insert data into mds table
  ret = insert_medium_info(40, 40);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = insert_medium_info(50, 50);
  ASSERT_EQ(OB_SUCCESS, ret);

  // query
  ret = ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  common::ObArenaAllocator arena_allocator;
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>>::value;
  ObTableScanParam scan_param;
  share::SCN read_snapshot = share::SCN::plus(share::SCN::min_scn(), 60);
  ret = ObMdsScanParamHelper::build_scan_param(
      arena_allocator,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      common::ObString()/*udf_key*/,
      false/*is_get*/,
      ObClockGenerator::getClock() + 1_s,
      read_snapshot,
      scan_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreCtx store_ctx;
  ObMdsRangeQueryIterator<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> range_query_iter;
  ret = tablet->mds_range_query<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      scan_param, store_ctx, range_query_iter);
  ASSERT_EQ(OB_SUCCESS, ret);

  mds::MdsDumpKV *kv = nullptr;
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(10, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(20, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(30, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(40, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObMediumCompactionInfoKey key;
    const common::ObString &str = kv->k_.key_;
    int64_t pos = 0;
    ret = key.mds_deserialize(str.ptr(), str.length(), pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(50, key.medium_snapshot_);
    range_query_iter.free_mds_kv(allocator_, kv);
  }
  {
    ret = range_query_iter.get_next_mds_kv(allocator_, kv);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_medium_info_iterator.log*");
  OB_LOGGER.set_file_name("test_medium_info_iterator.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}