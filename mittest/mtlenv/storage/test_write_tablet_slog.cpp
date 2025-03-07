// owner: gaishun.gs
// owner group: storage

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

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_tablet_helper.h"
#include "storage/test_dml_common.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

class TestWriteTabletSlog : public testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
  TestWriteTabletSlog();
  virtual void SetUp() override;
  virtual void TearDown() override;
public:
  static constexpr uint64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_LS_ID = 100;
  blocksstable::ObLogFileSpec log_file_spec_;
  share::ObLSID ls_id_;
  common::ObArenaAllocator allocator_;
  static observer::ObStartupAccelTaskHandler startup_accel_handler_;
};
observer::ObStartupAccelTaskHandler TestWriteTabletSlog::startup_accel_handler_;

TestWriteTabletSlog::TestWriteTabletSlog()
  : ls_id_(TEST_LS_ID)
{
}

void TestWriteTabletSlog::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  SERVER_STORAGE_META_SERVICE.is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestWriteTabletSlog::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

void TestWriteTabletSlog::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  log_file_spec_.retry_write_policy_ = "normal";
  log_file_spec_.log_create_policy_ = "normal";
  log_file_spec_.log_write_policy_ = "truncate";
}

void TestWriteTabletSlog::TearDown()
{
}

TEST_F(TestWriteTabletSlog, basic)
{
  ObLogCursor replay_start_cursor;
  ObTabletHandle invalid_tablet_handle;

  // create ls
  ObLSHandle handle;
  const ObLSID ls_id(TEST_LS_ID);
  ObLS *ls = NULL;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_svr);

  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // advance the replay start cursor
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantStorageMetaService*)->get_active_cursor(replay_start_cursor));

  // create tablet and write slog
  ObTabletID tablet_id(1001);

  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(handle, tablet_id, table_schema, allocator_));

  // mock minor freeze, assign tx data to tablet meta
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletCreateDeleteMdsUserData user_data;
  ASSERT_EQ(OB_SUCCESS, tablet->get_latest_committed(user_data));
  ASSERT_EQ(ObTabletStatus::NORMAL, user_data.tablet_status_.status_);

  // persist and transform tablet
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle new_tablet_hdl;
  const ObTabletPersisterParam param(ls_id, ls->get_ls_epoch(), tablet_id, 0 /*transfer_seq*/);
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_tablet_hdl));

  // write create tablet slog
  ObMetaDiskAddr disk_addr = new_tablet_hdl.get_obj()->tablet_addr_;
  ASSERT_EQ(OB_SUCCESS, TENANT_STORAGE_META_PERSISTER.update_tablet(ls_id, ls->get_ls_epoch(), tablet_id, disk_addr));

  // remove tablet without writing slog
  ASSERT_EQ(OB_SUCCESS, ls->ls_tablet_svr_.inner_remove_tablet(ls_id, tablet_id));
  ASSERT_NE(OB_SUCCESS, ls->get_tablet(tablet_id, invalid_tablet_handle));

  // update the start cursor for only replay tablet
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  ObTenantSuperBlock super_block = tenant->get_super_block();
  super_block.replay_start_point_ = replay_start_cursor;

  ObTenantStorageMetaReplayer &replayer = MTL(ObTenantStorageMetaService*)->get_replayer();
  if (!GCTX.is_shared_storage_mode()) {
    // the slogger has started when mtl start, start_replay will call start_log() again,
    // this error does not affect the accuracy of the replay result
    ASSERT_EQ(OB_INIT_TWICE, replayer.start_replay(super_block));
  } else {
    // TODO(fenggu.yh) add test for shared-storage
  }

  // check the result of replay
  ObTabletHandle replay_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, replay_tablet_handle));
  ObTablet *replay_tablet = replay_tablet_handle.get_obj();
  ASSERT_EQ(ls_id, replay_tablet->tablet_meta_.ls_id_);
  ASSERT_EQ(tablet_id, replay_tablet->tablet_meta_.tablet_id_);
  ASSERT_EQ(tablet_id, replay_tablet->tablet_meta_.data_tablet_id_);
  //ASSERT_EQ(1, replay_tablet->table_store_.major_tables_.count_);
}

}
}

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_write_tablet_slog.log*");
  OB_LOGGER.set_file_name("test_write_tablet_slog.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
