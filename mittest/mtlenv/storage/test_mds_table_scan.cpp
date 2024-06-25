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

#define protected public
#define private public

#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/storage/medium_info_helper.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "storage/tablet/ob_mds_row_iterator.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/tablet/ob_mds_schema_helper.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
class TestMdsTableScan : public::testing::Test
{
public:
  TestMdsTableScan() = default;
  virtual ~TestMdsTableScan() = default;
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int wait_for_mds_table_flush(const common::ObTabletID &tablet_id);
  static int wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id);
  static int try_schedule_mds_minor(const common::ObTabletID &tablet_id);
public:
  static constexpr uint64_t TENANT_ID = 1001;
  static const share::ObLSID LS_ID;
public:
  common::ObArenaAllocator allocator_;
};

const share::ObLSID TestMdsTableScan::LS_ID(1234);

void TestMdsTableScan::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = create_ls(TENANT_ID, LS_ID, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMdsTableScan::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int TestMdsTableScan::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestMdsTableScan::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int TestMdsTableScan::create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = 1234567;
  share::schema::ObTableSchema table_schema;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(build_test_schema(table_schema, table_id))) {
    LOG_WARN("failed to build table schema");
  } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_,
      ObTabletStatus::MAX, share::SCN::invalid_scn(), tablet_handle))) {
    LOG_WARN("failed to create tablet", K(ret));
  }

  return ret;
}

int TestMdsTableScan::get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), KP(ls));
  }

  return ret;
}

int TestMdsTableScan::wait_for_mds_table_flush(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int times = 0;
  share::SCN rec_scn = share::SCN::min_scn();
  // get before cnt
  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  EXPECT_NE(nullptr, tablet);
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ret = tablet->fetch_table_store(table_store_wrapper);
  EXPECT_EQ(OB_SUCCESS, ret);
  const ObTabletTableStore *table_store = table_store_wrapper.get_member();
  const int64_t mds_sstable_cnt_before = table_store->mds_sstables_.count();

  do
  {
    ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
    EXPECT_EQ(OB_SUCCESS, ret);
    tablet = tablet_handle.get_obj();
    EXPECT_NE(nullptr, tablet);

    mds::MdsTableHandle mds_table;
    ret = tablet->inner_get_mds_table(mds_table, false/*not_exist_create*/);
    ret = mds_table.get_rec_scn(rec_scn);
    EXPECT_EQ(OB_SUCCESS, ret);

    // sleep
    ::ob_usleep(100_ms);
    ++times;
  } while (OB_SUCCESS == ret && !rec_scn.is_max() && times < 20);

  EXPECT_TRUE(rec_scn.is_max());

  // check mds sstable
  ret = tablet->fetch_table_store(table_store_wrapper);
  EXPECT_EQ(OB_SUCCESS, ret);
  table_store = table_store_wrapper.get_member();
  EXPECT_EQ(mds_sstable_cnt_before + 1, table_store->mds_sstables_.count());

  if (::testing::Test::HasFailure()) {
    ret = OB_TIMEOUT;
  }

  return ret;
}

int TestMdsTableScan::wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int times = 0;
  int64_t node_cnt = INT64_MAX;

  do {
    ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
    EXPECT_EQ(OB_SUCCESS, ret);
    tablet = tablet_handle.get_obj();
    EXPECT_NE(nullptr, tablet);

    mds::MdsTableHandle mds_table;
    ret = tablet->inner_get_mds_table(mds_table, false/*not_exist_create*/);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = mds_table.get_node_cnt(node_cnt);
    EXPECT_EQ(OB_SUCCESS, ret);

    // sleep
    ::ob_usleep(100_ms);
    ++times;
  } while (OB_SUCCESS == ret && node_cnt != 0 && times < 60);
  EXPECT_EQ(0, node_cnt);

  if (::testing::Test::HasFailure()) {
    ret = OB_TIMEOUT;
  }

  return ret;
}

int TestMdsTableScan::try_schedule_mds_minor(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObApplyStatusGuard guard;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    int times = 0;
    do
    {
      STORAGE_LOG(INFO, "try mds minor", K(times));
      if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        LOG_WARN("failed to get ls", K(ret), K(LS_ID));
      } else if (OB_FAIL(TestMdsTableScan::get_tablet(tablet_id, tablet_handle))) {
        LOG_WARN("failed to tablet handle", K(ret), K(tablet_id));
      // for pass schedule varify
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret), KP(ls));
      } else if (OB_FAIL(ls->log_handler_.apply_service_->get_apply_status(LS_ID, guard))) {
        LOG_WARN("failed to get apply status", K(ret), K(LS_ID));
      } else if (OB_FALSE_IT(guard.apply_status_->max_applied_cb_scn_ = share::SCN::plus(share::SCN::min_scn(), 350+100))) {
      } else if (OB_FALSE_IT(guard.apply_status_->last_check_scn_ = share::SCN::plus(share::SCN::min_scn(), 350+100))) {
      } else if (OB_FALSE_IT(static_cast<palf::PalfHandleImpl* >(guard.apply_status_->palf_handle_.palf_handle_impl_)->sw_.lsn_allocator_.lsn_ts_meta_.scn_delta_ =450)) {
      } else if (OB_FAIL(compaction::ObTenantTabletScheduler::schedule_tablet_minor_merge<compaction::ObTabletMergeExecuteDag>(
          compaction::MDS_MINOR_MERGE, ls_handle, tablet_handle))) {
        STORAGE_LOG(WARN, "fail to schedule mds minor merge", K(ret));
      }
      // sleep
      ::ob_usleep(100_ms);
      ++times;
    } while (OB_EAGAIN == ret && times < 20);

    if (OB_SUCC(ret)) {
      times = 0;
      const ObTabletTableStore *table_store = nullptr;
      do {
        STORAGE_LOG(INFO, "waiting for mds minor finish", K(times));
        ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
        EXPECT_EQ(OB_SUCCESS, ret);
        ObTablet *tablet = tablet_handle.get_obj();
        // check mds sstable
        ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
        ret = tablet->fetch_table_store(table_store_wrapper);
        EXPECT_EQ(OB_SUCCESS, ret);
        table_store = table_store_wrapper.get_member();
        EXPECT_EQ(1, table_store->mds_sstables_.count());
        if (1 != table_store->mds_sstables_.count()) {
          ret = OB_EAGAIN;
          STORAGE_LOG(WARN, "fail to  finish mds minor", KPC(table_store));
        }
        ::ob_usleep(100_ms);
        ++times;
      } while (OB_EAGAIN == ret && times < 20);

      if (OB_SUCC(ret)) {
        STORAGE_LOG(INFO, "success finish mds minor", KPC(table_store));
      }
    } else {
      STORAGE_LOG(WARN, "fail to schedule minor merge", K(ret));
    }
  }

  return ret;
}

TEST_F(TestMdsTableScan, tablet_status)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_tablet_meta().mds_checkpoint_scn_;
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 50);
    ctx.on_redo(redo_scn);
    ctx.on_commit(create_commit_scn, create_commit_scn);
  }

  // mds table flush
  share::SCN decided_scn = share::SCN::plus(share::SCN::min_scn(), 110);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read mds sstable
  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();

  ObTableScanParam scan_param;
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletCreateDeleteMdsUserData>>::value;
  common::ObString dummy_key;
  const int64_t timeout_us = ObClockGenerator::getClock() + 1_s;
  share::SCN snapshot = share::SCN::plus(share::SCN::min_scn(), 120);
  ObStoreCtx store_ctx;
  ObMdsRowIterator iter;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      dummy_key,
      true/*is_get*/,
      timeout_us,
      snapshot,
      scan_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param, store_ctx, iter);
  ASSERT_EQ(OB_SUCCESS, ret);

  mds::MdsDumpKV kv;
  ret = iter.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_SUCCESS, ret);

  const common::ObString &str = kv.v_.user_data_;
  int64_t pos = 0;
  ObTabletCreateDeleteMdsUserData data;
  ret = data.deserialize(str.ptr(), str.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::NORMAL, data.tablet_status_);
  ASSERT_EQ(create_commit_scn, data.create_commit_scn_);
  ASSERT_EQ(100, data.create_commit_version_);

  kv.reset();
  ret = iter.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);

  kv.reset();
  snapshot = share::SCN::plus(share::SCN::min_scn(), 10);
  ret = tablet->read_raw_data(allocator_, mds_unit_id, dummy_key, snapshot, timeout_us, kv);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestMdsTableScan, aux_tablet_info)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_tablet_meta().mds_checkpoint_scn_;
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 50);
    ctx.on_redo(redo_scn);
    ctx.on_commit(create_commit_scn, create_commit_scn);
  }

  {
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(456)));
    ret = tablet->set_ddl_info(user_data, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 110);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 120);
    ctx.on_commit(commit_scn, commit_scn);
  }

  // mds table flush
  share::SCN decided_scn = share::SCN::plus(share::SCN::min_scn(), 130);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read mds sstable
  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();

  ObTableScanParam scan_param;
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;
  common::ObString dummy_key;
  const int64_t timeout_us = ObClockGenerator::getClock() + 1_s;
  share::SCN snapshot = share::SCN::plus(share::SCN::min_scn(), 130);
  ObStoreCtx store_ctx;
  ObMdsRowIterator iter;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      dummy_key,
      true/*is_get*/,
      timeout_us,
      snapshot,
      scan_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param, store_ctx, iter);
  ASSERT_EQ(OB_SUCCESS, ret);

  mds::MdsDumpKV kv;
  ret = iter.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_SUCCESS, ret);

  const common::ObString &str = kv.v_.user_data_;
  int64_t pos = 0;
  ObTabletBindingMdsUserData data;
  ret = data.deserialize(str.ptr(), str.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, data.data_tablet_id_.id_);
  ASSERT_EQ(101, data.hidden_tablet_id_.id_);
  ASSERT_EQ(9527, data.snapshot_version_);

  kv.reset();
  ret = iter.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestMdsTableScan, medium_info)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_tablet_meta().mds_checkpoint_scn_;
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 50);
    ctx.on_redo(redo_scn);
    ctx.on_commit(create_commit_scn, create_commit_scn);
  }

  {
    compaction::ObMediumCompactionInfoKey key(100);
    compaction::ObMediumCompactionInfo info;
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 100);
    ASSERT_EQ(OB_SUCCESS, ret);

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(777)));
    ret = tablet->set(key, info, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 110);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 120);
    ctx.on_commit(commit_scn, commit_scn);
  }

  {
    compaction::ObMediumCompactionInfoKey key(200);
    compaction::ObMediumCompactionInfo info;
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 200);
    ASSERT_EQ(OB_SUCCESS, ret);

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(888)));
    ret = tablet->set(key, info, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 210);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 220);
    ctx.on_commit(commit_scn, commit_scn);
  }

  // mds table flush
  share::SCN decided_scn = share::SCN::plus(share::SCN::min_scn(), 230);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read mds sstable
  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();

  ObArenaAllocator allocator;
  compaction::ObMediumCompactionInfo medium_info;
  auto func = [this, &medium_info](const compaction::ObMediumCompactionInfo& info) -> int {
    return medium_info.assign(this->allocator_, info);
  };
  share::SCN read_snapshot = share::SCN::plus(share::SCN::min_scn(), 250);
  ret = tablet->read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator,
      100,
      read_snapshot,
      1_s/*timeout_us*/,
      func);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, medium_info.medium_snapshot_);

  medium_info.reset();
  ret = tablet->read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator,
      105,
      read_snapshot,
      1_s/*timeout_us*/,
      func);
  ASSERT_EQ(OB_ITER_END, ret);

  medium_info.reset();
  ret = tablet->read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator,
      200,
      read_snapshot,
      1_s/*timeout_us*/,
      func);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(200, medium_info.medium_snapshot_);

  medium_info.reset();
  read_snapshot = share::SCN::plus(share::SCN::min_scn(), 150);
  ret = tablet->read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator,
      200,
      read_snapshot,
      1_s/*timeout_us*/,
      func);
  ASSERT_EQ(OB_ITER_END, ret);

  medium_info.reset();
  read_snapshot = share::SCN::plus(share::SCN::min_scn(), 150);
  ret = tablet->read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator,
      100,
      read_snapshot,
      1_s/*timeout_us*/,
      func);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, medium_info.medium_snapshot_);
}

TEST_F(TestMdsTableScan, multi_version_row)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_tablet_meta().mds_checkpoint_scn_;
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table no.1 row
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(create_commit_scn, create_commit_scn);
  }


  // write data to mds table no.2 row
  {
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(256)));
    ret = tablet->set_ddl_info(user_data, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 150);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 200);
    ctx.on_commit(commit_scn, commit_scn);
  }

  share::SCN transfer_out_commit_scn = share::SCN::plus(share::SCN::min_scn(), 300);
  // write data to mds table no.3 row
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
    user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(356)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(transfer_out_commit_scn, transfer_out_commit_scn);
  }

  // mds table flush
  share::SCN decided_scn;
  decided_scn = share::SCN::plus(share::SCN::min_scn(), 310);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletCreateDeleteMdsUserData>>::value;
  common::ObString dummy_key;
  const int64_t timeout_us = ObClockGenerator::getClock() + 1_s;

  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_EQ(ObTabletStatus::TRANSFER_OUT,tablet->tablet_meta_.last_persisted_committed_tablet_status_.tablet_status_);

  // get tablet status with 330
  ObTableScanParam scan_param;
  const share::SCN snapshot = share::SCN::plus(share::SCN::min_scn(), 330);
  ObStoreCtx store_ctx;
  ObMdsRowIterator iter;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      dummy_key,
      true/*is_get*/,
      timeout_us,
      snapshot,
      scan_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param, store_ctx, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  mds::MdsDumpKV kv;
  ret = iter.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_SUCCESS, ret);
  const common::ObString &str_330 = kv.v_.user_data_;
  int64_t pos = 0;
  ObTabletCreateDeleteMdsUserData data;
  ret = data.deserialize(str_330.ptr(), str_330.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::TRANSFER_OUT, data.tablet_status_);
  ASSERT_EQ(ObTabletMdsUserDataType::START_TRANSFER_OUT, data.data_type_);
  ASSERT_EQ(transfer_out_commit_scn, data.transfer_scn_);
  ASSERT_EQ(300, data.transfer_out_commit_version_);

  kv.reset();
  ret = iter.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);

  // get tablet status with 110
  ObTableScanParam scan_param2;
  const share::SCN snapshot2 = share::SCN::plus(share::SCN::min_scn(), 110);
  ObMdsRowIterator iter2;
  ObStoreCtx store_ctx2;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      common::ObString()/*udf_key*/,
      true/*is_get*/,
      timeout_us,
      snapshot2,
      scan_param2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param2, store_ctx2, iter2);
  ASSERT_EQ(OB_SUCCESS, ret);
  kv.reset();
  ret = iter2.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_SUCCESS, ret);
  const common::ObString &str_110 = kv.v_.user_data_;
  pos = 0;
  data.reset();
  ret = data.deserialize(str_110.ptr(), str_110.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::NORMAL, data.tablet_status_);
  ASSERT_EQ(ObTabletMdsUserDataType::CREATE_TABLET, data.data_type_);
  ASSERT_EQ(create_commit_scn, data.create_commit_scn_);
  ASSERT_EQ(100, data.create_commit_version_);
  kv.reset();
  ret = iter2.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);

  // get aux_tablet_info with 220
  ObTableScanParam scan_param3;
  constexpr uint8_t mds_unit_id3 = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;
  const share::SCN snapshot3 = share::SCN::plus(share::SCN::min_scn(), 220);
  ObStoreCtx store_ctx3;
  ObMdsRowIterator iter3;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id3,
      dummy_key,
      true/*is_get*/,
      timeout_us,
      snapshot3,
      scan_param3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param3, store_ctx3, iter3);
  ASSERT_EQ(OB_SUCCESS, ret);
  kv.reset();
  ret = iter3.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_SUCCESS, ret);
  const common::ObString &str_220 = kv.v_.user_data_;
  pos = 0;
  ObTabletBindingMdsUserData data3;
  ret = data3.deserialize(str_220.ptr(), str_220.length(), pos);
  ASSERT_EQ(100, data3.data_tablet_id_.id_);
  ASSERT_EQ(101, data3.hidden_tablet_id_.id_);
  ASSERT_EQ(9527, data3.snapshot_version_);

  kv.reset();
  ret = iter3.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);

  // get aux_tablet_info with 190
  ObTableScanParam scan_param4;
  constexpr uint8_t mds_unit_id4 = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;
  const share::SCN snapshot4 = share::SCN::plus(share::SCN::min_scn(), 190);
  ObStoreCtx store_ctx4;
  ObMdsRowIterator iter4;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id4,
      dummy_key,
      true/*is_get*/,
      timeout_us,
      snapshot4,
      scan_param4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param4, store_ctx4, iter4);
  ASSERT_EQ(OB_SUCCESS, ret);
  kv.reset();
  ret = iter4.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestMdsTableScan, test_minor_scan)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_tablet_meta().mds_checkpoint_scn_;
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table no.1 row
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(create_commit_scn, create_commit_scn);
  }

  // write data to mds table no.2 row
  {
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(256)));
    ret = tablet->set_ddl_info(user_data, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 150);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 200);
    ctx.on_commit(commit_scn, commit_scn);
  }


  // mds table flush once
  share::SCN decided_scn;
  decided_scn = share::SCN::plus(share::SCN::min_scn(), 250);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);


  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  // write data to mds table no.3 row
  {
    compaction::ObMediumCompactionInfoKey key(250);
    compaction::ObMediumCompactionInfo info;
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 250);
    ASSERT_EQ(OB_SUCCESS, ret);

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(300)));
    ret = tablet->set(key, info, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 260);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 270);
    ctx.on_commit(commit_scn, commit_scn);
  }
  // write data to mds table no.4 row
  {
    compaction::ObMediumCompactionInfoKey key(270);
    compaction::ObMediumCompactionInfo info;
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 270);
    ASSERT_EQ(OB_SUCCESS, ret);

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(320)));
    ret = tablet->set(key, info, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 280);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 290);
    ctx.on_commit(commit_scn, commit_scn);
  }

  share::SCN transfer_out_commit_scn = share::SCN::plus(share::SCN::min_scn(), 300);
  // write data to mds table no.5 row
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
    user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(356)));
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.single_log_commit(transfer_out_commit_scn, transfer_out_commit_scn);
  }

  // mds table flush twice ,hope to build minor sstable
  decided_scn = share::SCN::plus(share::SCN::min_scn(), 350);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush except->triggle minor
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // schedule mds minor
  ASSERT_EQ(OB_SUCCESS, try_schedule_mds_minor(tablet_id));
  // test multi version get
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletCreateDeleteMdsUserData>>::value;
  common::ObString dummy_key;
  const int64_t timeout_us = ObClockGenerator::getClock() + 1_s;

  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // check mds sstable be minor
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ret = tablet->fetch_table_store(table_store_wrapper);
  EXPECT_EQ(OB_SUCCESS, ret);
  const ObTabletTableStore *table_store = table_store_wrapper.get_member();
  EXPECT_EQ(1, table_store->mds_sstables_.count());
  EXPECT_EQ(ObITable::TableType::MDS_MINOR_SSTABLE, table_store->mds_sstables_.at(0)->key_.table_type_);

  // get tablet status with 330
  ObTableScanParam scan_param;
  const share::SCN snapshot = share::SCN::plus(share::SCN::min_scn(), 330);
  ObStoreCtx store_ctx;
  ObMdsRowIterator iter;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      dummy_key,
      true/*is_get*/,
      timeout_us,
      snapshot,
      scan_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param, store_ctx, iter);
  ASSERT_EQ(OB_SUCCESS, ret);
  mds::MdsDumpKV kv;
  ret = iter.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_SUCCESS, ret);
  const common::ObString &str_330 = kv.v_.user_data_;
  int64_t pos = 0;
  ObTabletCreateDeleteMdsUserData data;
  ret = data.deserialize(str_330.ptr(), str_330.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::TRANSFER_OUT, data.tablet_status_);
  ASSERT_EQ(ObTabletMdsUserDataType::START_TRANSFER_OUT, data.data_type_);
  ASSERT_EQ(transfer_out_commit_scn, data.transfer_scn_);
  ASSERT_EQ(300, data.transfer_out_commit_version_);

  kv.reset();
  ret = iter.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);

  // get tablet status with 110
  ObTableScanParam scan_param2;
  const share::SCN snapshot2 = share::SCN::plus(share::SCN::min_scn(), 110);
  ObMdsRowIterator iter2;
  ObStoreCtx store_ctx2;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id,
      common::ObString()/*udf_key*/,
      true/*is_get*/,
      timeout_us,
      snapshot2,
      scan_param2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param2, store_ctx2, iter2);
  ASSERT_EQ(OB_SUCCESS, ret);
  kv.reset();
  ret = iter2.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_SUCCESS, ret);
  const common::ObString &str_110 = kv.v_.user_data_;
  pos = 0;
  data.reset();
  ret = data.deserialize(str_110.ptr(), str_110.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::NORMAL, data.tablet_status_);
  ASSERT_EQ(ObTabletMdsUserDataType::CREATE_TABLET, data.data_type_);
  ASSERT_EQ(create_commit_scn, data.create_commit_scn_);
  ASSERT_EQ(100, data.create_commit_version_);
  kv.reset();
  ret = iter2.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);

  // get aux_tablet_info with 220
  ObTableScanParam scan_param3;
  constexpr uint8_t mds_unit_id3 = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;
  const share::SCN snapshot3 = share::SCN::plus(share::SCN::min_scn(), 220);
  ObStoreCtx store_ctx3;
  ObMdsRowIterator iter3;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id3,
      common::ObString()/*udf_key*/,
      true/*is_get*/,
      timeout_us,
      snapshot3,
      scan_param3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param3, store_ctx3, iter3);
  ASSERT_EQ(OB_SUCCESS, ret);
  kv.reset();
  ret = iter3.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_SUCCESS, ret);
  const common::ObString &str_220 = kv.v_.user_data_;
  pos = 0;
  ObTabletBindingMdsUserData data3;
  ret = data3.deserialize(str_220.ptr(), str_220.length(), pos);
  ASSERT_EQ(100, data3.data_tablet_id_.id_);
  ASSERT_EQ(101, data3.hidden_tablet_id_.id_);
  ASSERT_EQ(9527, data3.snapshot_version_);

  kv.reset();
  ret = iter3.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);

  // get aux_tablet_info with 190
  ObTableScanParam scan_param4;
  constexpr uint8_t mds_unit_id4 = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;
  const share::SCN snapshot4 = share::SCN::plus(share::SCN::min_scn(), 190);
  ObStoreCtx store_ctx4;
  ObMdsRowIterator iter4;

  ret = ObMdsScanParamHelper::build_scan_param(
      allocator_,
      LS_ID,
      tablet_id,
      ObMdsSchemaHelper::MDS_TABLE_ID,
      mds_unit_id4,
      common::ObString()/*udf_key*/,
      true/*is_get*/,
      timeout_us,
      snapshot4,
      scan_param4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = tablet->mds_table_scan(scan_param4, store_ctx4, iter4);
  ASSERT_EQ(OB_SUCCESS, ret);
  kv.reset();
  ret = iter4.get_next_mds_kv(allocator_, kv);
  ASSERT_EQ(OB_ITER_END, ret);

  // get medium_compaction_info
  ObArenaAllocator allocator;
  compaction::ObMediumCompactionInfo medium_info;
  auto func = [this, &medium_info](const compaction::ObMediumCompactionInfo& info) -> int {
    return medium_info.assign(this->allocator_, info);
  };
  share::SCN read_snapshot = share::SCN::plus(share::SCN::min_scn(), 300);
  ret = tablet->read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator,
      250,
      read_snapshot,
      1_s/*timeout_us*/,
      func);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(250, medium_info.medium_snapshot_);


  medium_info.reset();
  ret = tablet->read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator,
      270,
      read_snapshot,
      1_s/*timeout_us*/,
      func);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(270, medium_info.medium_snapshot_);

  medium_info.reset();
  read_snapshot = share::SCN::plus(share::SCN::min_scn(), 250);
  ret = tablet->read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator,
      250,
      read_snapshot,
      1_s/*timeout_us*/,
      func);
  ASSERT_EQ(OB_ITER_END, ret);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_table_scan.log*");
  OB_LOGGER.set_file_name("test_mds_table_scan.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}