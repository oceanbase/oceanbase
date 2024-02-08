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
#include "mtlenv/mock_tenant_module_env.h"
#include "common/log/ob_log_cursor.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_log_struct.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_slog_helper.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/test_dml_common.h"
#include "storage/slog_ckpt/ob_tablet_replay_create_handler.h"

#include "lib/oblog/ob_log.h"
#include "share/ob_force_print_log.h"

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

  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  ASSERT_EQ(OB_SUCCESS, startup_accel_handler_.init(observer::SERVER_ACCEL));
  ASSERT_EQ(OB_SUCCESS, startup_accel_handler_.start());

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

  startup_accel_handler_.destroy();
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
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  ASSERT_NE(nullptr, slogger);
  ObLogCursor replay_start_cursor;
  ObLogCursor replay_finish_cursor;
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
  ASSERT_EQ(OB_SUCCESS, slogger->get_active_cursor(replay_start_cursor));

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
  ASSERT_EQ(OB_SUCCESS, tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data));
  ASSERT_EQ(ObTabletStatus::NORMAL, user_data.tablet_status_.status_);

  // persist and transform tablet
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle new_tablet_hdl;
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_hdl));

  // write create tablet slog
  ObMetaDiskAddr disk_addr = new_tablet_hdl.get_obj()->tablet_addr_;
  ASSERT_EQ(OB_SUCCESS, ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr));

  // remove tablet without writing slog
  ASSERT_EQ(OB_SUCCESS, ls->ls_tablet_svr_.inner_remove_tablet(ls_id, tablet_id));
  ASSERT_NE(OB_SUCCESS, ls->get_tablet(tablet_id, invalid_tablet_handle));

  // replay create tablet
  ObTenantCheckpointSlogHandler *slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  ASSERT_EQ(OB_SUCCESS, slog_handler->replay_tablet_disk_addr_map_.create(10003, "TestTabletSlog"));
  ObStorageLogReplayer log_replayer;
  ASSERT_EQ(OB_SUCCESS, log_replayer.init(slogger->get_dir(), log_file_spec_));
  ASSERT_EQ(OB_SUCCESS, log_replayer.register_redo_module(
      ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, slog_handler));
  ASSERT_EQ(OB_SUCCESS, log_replayer.replay(replay_start_cursor, replay_finish_cursor, OB_SERVER_TENANT_ID));
  ObTabletReplayCreateHandler handler;
  ASSERT_EQ(OB_SUCCESS, handler.init(slog_handler->replay_tablet_disk_addr_map_, ObTabletRepalyOperationType::REPLAY_CREATE_TABLET));
  ASSERT_EQ(OB_SUCCESS, handler.concurrent_replay(&startup_accel_handler_));

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
