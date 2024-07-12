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
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"


#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
class TestMdsCompat : public::testing::Test
{
public:
  TestMdsCompat() = default;
  virtual ~TestMdsCompat() = default;
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

const share::ObLSID TestMdsCompat::LS_ID(1234);

void TestMdsCompat::SetUpTestCase()
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

void TestMdsCompat::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int TestMdsCompat::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestMdsCompat::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int TestMdsCompat::create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
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

int TestMdsCompat::get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
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

int TestMdsCompat::wait_for_mds_table_flush(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int times = 0;
  share::SCN rec_scn = share::SCN::min_scn();
  // get before cnt
  ret = TestMdsCompat::get_tablet(tablet_id, tablet_handle);
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
    ret = TestMdsCompat::get_tablet(tablet_id, tablet_handle);
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

int TestMdsCompat::wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int times = 0;
  int64_t node_cnt = INT64_MAX;

  do {
    ret = TestMdsCompat::get_tablet(tablet_id, tablet_handle);
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

TEST_F(TestMdsCompat, migration_param)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 50);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_tablet_meta().mds_checkpoint_scn_;
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);
  share::SCN invalid_scn;
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


  // write data to mds table no.3 row
  {
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;

    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(1000)));
    ret = tablet->set_ddl_info(user_data, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 250);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 280);
    ctx.on_commit(commit_scn, commit_scn);
  }

  {
    share::ObTabletAutoincSeq user_data;
    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(1200)));
    ret = tablet->set(user_data, ctx, 1_s/*lock_timeout_us*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN redo_scn = share::SCN::plus(share::SCN::min_scn(), 300);
    ctx.on_redo(redo_scn);
    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 320);
    ctx.on_commit(commit_scn, commit_scn);
  }

  // mock mds data
  ObTabletMdsData mds_table_data;
  ObTabletMdsData base_data;
  base_data.init_for_first_creation();
  ObTabletMdsData mocked_mds_data;
  ret = tablet->read_mds_table(allocator_, mds_table_data, false/*for_flush*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("read mds table", K(ret), K(mds_table_data));
  ret = mocked_mds_data.init_for_mds_table_dump(allocator_, mds_table_data, base_data, 0/*finish_medium_scn*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mds table flush
  share::SCN decided_scn;
  decided_scn = share::SCN::plus(share::SCN::min_scn(), 410);
  ret = tablet->mds_table_flush(decided_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestMdsCompat::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ObMigrationTabletParam param;
  ret = tablet->build_migration_tablet_param(param);
  param.last_persisted_committed_tablet_status_.on_init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(param.is_valid());

  // replace mds data in migration tablet param with our mocked mds data
  param.mds_data_.reset();
  ret = param.mds_data_.init(param.allocator_, mocked_mds_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("start generate mds sstable from param", K(ret), K(param));
  ObTableHandleV2 table_handle;
  ret = ObMdsDataCompatHelper::generate_mds_mini_sstable(param, allocator_, table_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  const ObITable *table = table_handle.get_table();
  ASSERT_NE(nullptr, table);
  ASSERT_EQ(share::SCN::plus(share::SCN::min_scn(), 1), table->get_start_scn());
  ASSERT_EQ(param.mds_checkpoint_scn_, table->get_end_scn());
}

TEST_F(TestMdsCompat, compat)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 80);
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

  // mock mds data
  ObTabletMdsData mds_table_data;
  ObTabletMdsData base_data;
  base_data.init_for_first_creation();
  ObTabletMdsData mocked_mds_data;
  ret = tablet->read_mds_table(allocator_, mds_table_data, false/*for_flush*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("read mds table", K(ret), K(mds_table_data));
  ret = mocked_mds_data.init_for_mds_table_dump(allocator_, mds_table_data, base_data, 0/*finish_medium_scn*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  // assign to tablet
  ASSERT_EQ(nullptr, tablet->mds_data_);
  ret = ObTabletObjLoadHelper::alloc_and_new(allocator_, tablet->mds_data_);
  ASSERT_NE(nullptr, tablet->mds_data_);
  ret = tablet->mds_data_->init_for_evict_medium_info(allocator_, mocked_mds_data, 0/*finish_medium_scn*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  // convert to mds sstable
  ObArenaAllocator allocator;
  ObTableHandleV2 table_handle;
  ret = ObMdsDataCompatHelper::generate_mds_mini_sstable(*tablet_handle.get_obj(), allocator, table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  blocksstable::ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(sstable));
  ASSERT_NE(nullptr, sstable);
  ASSERT_TRUE(sstable->is_valid());

  // free memory for mds data
  tablet->mds_data_->~ObTabletMdsData();
  ObTabletObjLoadHelper::free(allocator_, tablet->mds_data_);
  ASSERT_EQ(nullptr, tablet->mds_data_);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_compat.log*");
  OB_LOGGER.set_file_name("test_mds_compat.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}