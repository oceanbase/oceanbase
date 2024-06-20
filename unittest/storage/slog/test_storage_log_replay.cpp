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
#include "lib/file/ob_file.h"
#include "lib/ob_define.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/slog/simple_ob_storage_redo_module.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_simple_mem_limit_getter.h"
#include <thread>

#define private public
#include "share/rc/ob_tenant_base.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_log_reader.h"
#include "storage/slog/ob_storage_logger_manager.h"
#undef private

namespace oceanbase
{
using namespace common;
static ObSimpleMemLimitGetter getter;

namespace storage
{

class TestStorageLogReplay : public TestDataFilePrepare
{
public:
  TestStorageLogReplay()
    : TestDataFilePrepare(&getter, "TestStorageLogReplay")
  {
  }
  virtual ~TestStorageLogReplay() = default;

  virtual void SetUp() override;
  virtual void TearDown();
  void build_storage(int64_t cnt);

public:
  static const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;

public:
  ObStorageLogReplayer replayer_;
  char dir_[128];
  ObLogCursor replay_start_cursor_;
  ObLogCursor replay_finish_cursor_;
  blocksstable::ObLogFileSpec log_file_spec_;
  SimpleObStorageModule tenant_storage_;
};

void TestStorageLogReplay::SetUp()
{
  system("rm -rf ./test_storage_log_replay");
  MEMCPY(dir_, "./test_storage_log_replay", sizeof("./test_storage_log_replay"));
  replay_start_cursor_.file_id_ = 1;
  replay_start_cursor_.log_id_ = 1;
  replay_start_cursor_.offset_ = 0;
  log_file_spec_.retry_write_policy_ = "normal";
  log_file_spec_.log_create_policy_ = "normal";
  log_file_spec_.log_write_policy_ = "truncate";
  TestDataFilePrepare::TearDown();
  TestDataFilePrepare::SetUp();
  FileDirectoryUtils::create_full_path("./test_storage_log_replay");
  SLOGGERMGR.init(dir_, dir_, MAX_FILE_SIZE, log_file_spec_);
}

void TestStorageLogReplay::TearDown()
{
  SLOGGERMGR.destroy();
  system("rm -rf ./test_storage_log_replay");
  TestDataFilePrepare::TearDown();
}

void TestStorageLogReplay::build_storage(int64_t cnt)
{
  tenant_storage_.slog_cnt_ = cnt;
  for (int i = 0; i < cnt; i++) {
    tenant_storage_.slogs_[i].block_cnt_ = ObRandom::rand(1, 1024);
    for (int j = 0; j < tenant_storage_.slogs_[i].block_cnt_; j++) {
      tenant_storage_.slogs_[i].blocks_[j] = ObRandom::rand(0, 10<<20);
    }
  }
}

TEST_F(TestStorageLogReplay, test_basic)
{
  int ret = OB_SUCCESS;
  SimpleObStorageModule redo_module;

  // test invalid initialization
  ret = replayer_.init(nullptr, log_file_spec_);
  ASSERT_NE(OB_SUCCESS, ret);
  // test invalid unregister
  ret = replayer_.unregister_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE);
  ASSERT_NE(OB_SUCCESS, ret);
  // test invalid register
  ret = replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module);

  // test normal initialization
  ret = replayer_.init(dir_, log_file_spec_);
  ASSERT_EQ(OB_SUCCESS, ret);
  // test no redo log
  ret = replayer_.replay(replay_start_cursor_, replay_finish_cursor_, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, replay_finish_cursor_.file_id_);
  ASSERT_EQ(1, replay_finish_cursor_.log_id_);
  ASSERT_EQ(0, replay_finish_cursor_.offset_);

  // test normal replay (single write)
  build_storage(ObRandom::rand(1, 127));

  ObStorageLogger *tmp_slogger = OB_NEW(ObStorageLogger, ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->init(SLOGGERMGR, 500));
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->start());

  ObTenantBase tenant_base(10);
  tenant_base.set(tmp_slogger);
  ObTenantEnv::set_tenant(&tenant_base);
  ASSERT_EQ(OB_SUCCESS, tenant_base.init());

  ObTenantSwitchGuard guard;
  guard.switch_to(10);
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  slogger->is_start_ = false;
  slogger->start_log(replay_start_cursor_);

  ObStorageLogParam log_param;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_LS);
  for (int i = 0; i < tenant_storage_.slog_cnt_; i++) {
    log_param.data_ = &tenant_storage_.slogs_[i];
    ret = slogger->write_log(log_param);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  replayer_.destroy();
  ret = replayer_.init(slogger->get_dir(), log_file_spec_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.replay(replay_start_cursor_, replay_finish_cursor_, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tenant_storage_ == redo_module);
  ret = replayer_.unregister_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  replayer_.destroy();
  redo_module.reset();


  // test normal replay (batch write)
  build_storage(ObRandom::rand(1, 127));

  // mock drop tenant
  tmp_slogger->~ObStorageLogger();
  OB_DELETE(ObStorageLogger, ObModIds::TEST, tmp_slogger);

  tmp_slogger = OB_NEW(ObStorageLogger, ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->init(SLOGGERMGR, 500));
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->start());

  tenant_base.set(tmp_slogger);
  ObTenantEnv::set_tenant(&tenant_base);

  guard.switch_to(10);
  slogger = MTL(ObStorageLogger*);
  slogger->is_start_ = false;
  slogger->start_log(replay_finish_cursor_);

  ObSEArray<ObStorageLogParam, 10> param_arr;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_LS);
  for (int i = 0; i < tenant_storage_.slog_cnt_; i++) {
    log_param.data_ = &(tenant_storage_.slogs_[i]);
    param_arr.push_back(log_param);
  }
  ret = slogger->get_active_cursor(replay_start_cursor_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = slogger->write_log(param_arr);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = replayer_.init(slogger->get_dir(), log_file_spec_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.replay(replay_start_cursor_, replay_finish_cursor_, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tenant_storage_ == redo_module);
  ret = replayer_.unregister_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  replayer_.destroy();
  redo_module.reset();


  // test different sub_type and checkpoint
  build_storage(ObRandom::rand(1, 127));
  slogger->get_active_cursor(replay_start_cursor_);

  for (int i = 0; i < tenant_storage_.slog_cnt_; i++) {
    log_param.data_ = &tenant_storage_.slogs_[i];
    ret = slogger->write_log(log_param);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  tenant_storage_.slog_cnt_++;
  int tmp_cnt = tenant_storage_.slog_cnt_ - 1;
  tenant_storage_.slogs_[tmp_cnt].blocks_[0] = 3214;
  tenant_storage_.slogs_[tmp_cnt].block_cnt_ = 1;
  log_param.data_ = &(tenant_storage_.slogs_[tmp_cnt]);
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_LS);
  ret = slogger->write_log(log_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = replayer_.init(slogger->get_dir(), log_file_spec_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.replay(replay_start_cursor_, replay_finish_cursor_, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(tenant_storage_ == redo_module);

  for (int i = 0; i < tmp_cnt; i++) {
    ASSERT_TRUE(tenant_storage_.slogs_[i] == redo_module.slogs_[i]);
  }
  ASSERT_TRUE(tenant_storage_.slogs_[tmp_cnt] != redo_module.slogs_[tmp_cnt]);
}

TEST_F(TestStorageLogReplay, test_switch_file_replay)
{
  // replay start cursor is the end of the first file and slogs are dumped to the second file
  int ret = OB_SUCCESS;
  ObLogCursor write_start_cursor;
  write_start_cursor.file_id_ = 1;
  write_start_cursor.log_id_ = 1;
  SimpleObStorageModule redo_module;

  ObStorageLogger *tmp_slogger = OB_NEW(ObStorageLogger, ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->init(SLOGGERMGR, 500));
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->start());

  ObTenantBase tenant_base(10);
  tenant_base.set(tmp_slogger);
  ObTenantEnv::set_tenant(&tenant_base);
  ASSERT_EQ(OB_SUCCESS, tenant_base.init());

  ObTenantSwitchGuard guard;
  guard.switch_to(10);
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  slogger->is_start_ = false;
  slogger->start_log(write_start_cursor);

  ObStorageLogParam log_param;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_LS);

  build_storage(ObRandom::rand(1, 127));
  for (int i = 0; i < tenant_storage_.slog_cnt_; i++) {
    log_param.data_ = &tenant_storage_.slogs_[i];
    ret = slogger->write_log(log_param);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  slogger->get_active_cursor(replay_start_cursor_);

  build_storage(ObRandom::rand(1, 127));
  write_start_cursor.file_id_ = 2;
  write_start_cursor.log_id_ = replay_start_cursor_.log_id_;
  write_start_cursor.offset_ = 0;

  // mock drop tenant
  tmp_slogger->~ObStorageLogger();
  OB_DELETE(ObStorageLogger, ObModIds::TEST, tmp_slogger);

  tmp_slogger = OB_NEW(ObStorageLogger, ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->init(SLOGGERMGR, 500));
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->start());

  tenant_base.set(tmp_slogger);
  ObTenantEnv::set_tenant(&tenant_base);

  guard.switch_to(10);
  slogger = MTL(ObStorageLogger*);
  slogger->is_start_ = false;
  slogger->start_log(write_start_cursor);

  for (int i = 0; i < tenant_storage_.slog_cnt_; i++) {
    log_param.data_ = &tenant_storage_.slogs_[i];
    ret = slogger->write_log(log_param);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = replayer_.init(slogger->get_dir(), log_file_spec_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = replayer_.replay(replay_start_cursor_, replay_finish_cursor_, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tenant_storage_ == redo_module);
}

TEST_F(TestStorageLogReplay, test_mock_restart)
{
  int ret = OB_SUCCESS;
  ObLogCursor write_start_cursor;
  write_start_cursor.file_id_ = 1;
  write_start_cursor.log_id_ = 1;
  SimpleObStorageModule redo_module;

  ObStorageLogger *tmp_slogger = OB_NEW(ObStorageLogger, ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->init(SLOGGERMGR, 500));
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->start());

  ObTenantBase tenant_base(10);
  tenant_base.set(tmp_slogger);
  ObTenantEnv::set_tenant(&tenant_base);
  ASSERT_EQ(OB_SUCCESS, tenant_base.init());

  ObTenantSwitchGuard guard;
  guard.switch_to(10);
  ObStorageLogger *slogger = MTL(ObStorageLogger*);
  slogger->is_start_ = false;
  slogger->start_log(write_start_cursor);

  ObStorageLogParam log_param;
  log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_LS);

  build_storage(40);
  // first time to write slog
  for (int i = 0; i < tenant_storage_.slog_cnt_; i++) {
    log_param.data_ = &tenant_storage_.slogs_[i];
    ASSERT_EQ(OB_SUCCESS, slogger->write_log(log_param));
  }
  // replay first slog file
  ASSERT_EQ(OB_SUCCESS, replayer_.init(slogger->get_dir(), log_file_spec_));
  ASSERT_EQ(OB_SUCCESS, replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module));
  ASSERT_EQ(OB_SUCCESS, replayer_.replay(replay_start_cursor_, replay_finish_cursor_, OB_SERVER_TENANT_ID));
  replayer_.destroy();
  redo_module.reset();

  // reset slogger and set its start cursor as replay_finish_cursor
  slogger->destroy();
  ASSERT_EQ(OB_SUCCESS, slogger->init(SLOGGERMGR, 500));
  ASSERT_EQ(OB_SUCCESS, slogger->start());
  slogger->is_start_ = false;
  ASSERT_EQ(OB_SUCCESS, slogger->start_log(replay_finish_cursor_));

  build_storage(30);
  // second time to write slog
  for (int i = 0; i < tenant_storage_.slog_cnt_; i++) {
    log_param.data_ = &tenant_storage_.slogs_[i];
    ASSERT_EQ(OB_SUCCESS, slogger->write_log(log_param));
  }
  // replay first and second slog files
  ASSERT_EQ(OB_SUCCESS, replayer_.init(slogger->get_dir(), log_file_spec_));
  ASSERT_EQ(OB_SUCCESS, replayer_.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &redo_module));
  ASSERT_EQ(OB_SUCCESS, replayer_.replay(replay_start_cursor_, replay_finish_cursor_, OB_SERVER_TENANT_ID));
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_log_replay.log*");
  OB_LOGGER.set_file_name("test_storage_log_replay.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
