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
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/test_dml_common.h"
#include "observer/ob_safe_destroy_thread.h"

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
};

TestWriteTabletSlog::TestWriteTabletSlog()
  : ls_id_(TEST_LS_ID)
{
}

void TestWriteTabletSlog::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  SAFE_DESTROY_INSTANCE.init();
  SAFE_DESTROY_INSTANCE.start();
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestWriteTabletSlog::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID), false);
  ASSERT_EQ(OB_SUCCESS, ret);

  SAFE_DESTROY_INSTANCE.stop();
  SAFE_DESTROY_INSTANCE.wait();
  SAFE_DESTROY_INSTANCE.destroy();

  MockTenantModuleEnv::get_instance().destroy();
}

void TestWriteTabletSlog::SetUp()
{
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
  obrpc::ObBatchCreateTabletArg tablet_arg;
  ObTabletID tablet_id(1001);
  ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(TEST_TENANT_ID, ls_id, tablet_id, tablet_arg));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(*ls->get_tablet_svr(), tablet_arg));

  // mock minor freeze, assign tx data to tablet meta
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletTxMultiSourceDataUnit tx_data;
  ASSERT_EQ(OB_SUCCESS, tablet->get_tx_data(tx_data));
  ASSERT_EQ(ObTabletStatus::NORMAL, tx_data.tablet_status_);
  tablet->tablet_meta_.tx_data_ = tx_data;

  // write create tablet slog
  ObMetaDiskAddr disk_addr;
  ASSERT_EQ(OB_SUCCESS, ObTabletSlogHelper::write_create_tablet_slog(tablet_handle, disk_addr));

  // remove tablet without writing slog
  ASSERT_EQ(OB_SUCCESS, ls->ls_tablet_svr_.do_remove_tablet(ls_id, tablet_id));
  ASSERT_NE(OB_SUCCESS, ls->get_tablet(tablet_id, invalid_tablet_handle));

  // replay create tablet
  ObTenantCheckpointSlogHandler *slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  ASSERT_EQ(OB_SUCCESS, slog_handler->replay_tablet_disk_addr_map_.create(10003, "TestTabletSlog"));
  ObStorageLogReplayer log_replayer;
  ASSERT_EQ(OB_SUCCESS, log_replayer.init(slogger->get_dir(), log_file_spec_));
  ASSERT_EQ(OB_SUCCESS, log_replayer.register_redo_module(
      ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, slog_handler));
  ASSERT_EQ(OB_SUCCESS, log_replayer.replay(replay_start_cursor, replay_finish_cursor, OB_SERVER_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, slog_handler->replay_load_tablets());

  // check the result of replay
  ObTabletHandle replay_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, replay_tablet_handle));
  ObTablet *replay_tablet = replay_tablet_handle.get_obj();
  ASSERT_EQ(ls_id, replay_tablet->tablet_meta_.ls_id_);
  ASSERT_EQ(tablet_id, replay_tablet->tablet_meta_.tablet_id_);
  ASSERT_EQ(tablet_id, replay_tablet->tablet_meta_.data_tablet_id_);
  ASSERT_EQ(1, replay_tablet->table_store_.major_tables_.count_);
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
