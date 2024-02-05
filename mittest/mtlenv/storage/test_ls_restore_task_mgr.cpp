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

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define private public
#define protected public
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/init_basic_struct.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "share/partition_table/fake_part_property_getter.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share;
using namespace testing;
using namespace storage;
using namespace host;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace storage
{

int ObLS::online()
{
  return OB_SUCCESS;
}

int ObLSRestoreHandler::check_meta_tenant_normal_(bool &is_normal) {
  is_normal = true;
  return OB_SUCCESS;
}

int ObLSRestoreHandler::fill_restore_arg_()
{
  int ret = OB_SUCCESS;
  share::ObRestoreBackupSetBriefInfo info;
  info.backup_set_path_.assign("file:///data/backup_set_1_full/");
  info.backup_set_desc_.backup_set_id_ = 1;
  info.backup_set_desc_.backup_type_.type_ = share::ObBackupType::FULL_BACKUP;
  ls_restore_arg_.job_id_ = 1;
  ls_restore_arg_.restore_type_ = share::ObRestoreType::NORMAL_RESTORE;
  ls_restore_arg_.tenant_id_ = MTL_ID();
  ls_restore_arg_.restore_scn_.convert_for_logservice(10000000000);
  ls_restore_arg_.backup_cluster_version_ = 10;
  ls_restore_arg_.backup_set_list_.push_back(info);
  ls_restore_arg_.backup_piece_list_.push_back("file:///archive/piece");
  return ret;
}

void ObLSRestoreHandler::wakeup()
{
}

int ObLSRestoreTaskMgr::check_task_exist_(const share::ObTaskId &task_id,
    bool &is_exist)
{
  UNUSEDx(task_id);
  is_exist = false;
  return OB_SUCCESS;
}

bool global_restored = false;
int ObLSRestoreTaskMgr::check_tablet_deleted_or_restored_(storage::ObLS &ls, const common::ObTabletID &tablet_id, bool &is_deleted, bool &is_restored)
{
  is_deleted = false;
  is_restored = global_restored;
  return OB_SUCCESS;
}

int ObILSRestoreState::insert_initial_ls_restore_progress_()
{
  return OB_SUCCESS;
}

int ObILSRestoreState::report_ls_restore_progress_(storage::ObLS &ls, const share::ObLSRestoreStatus &status,
      const share::ObTaskId &trace_id, const int result, const char *comment)
{
  UNUSEDx(ls, status, trace_id, result, comment);
  return OB_SUCCESS;
}

}

namespace unittest
{

class FakeLS : public storage::ObLS
{
public:
  FakeLS(const int64_t &ls_id) {
    ls_meta_.tenant_id_ = 1001;
    ls_meta_.ls_id_ = ObLSID(ls_id);
    ls_meta_.migration_status_ = storage::ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
    ls_meta_.gc_state_ = logservice::LSGCState::NORMAL;
    ls_meta_.restore_status_ = ObLSRestoreStatus::Status::RESTORE_TABLETS_META;
  }
};

class TestRestoreTaskMgr : public testing::Test
{
public:
  virtual void SetUp() {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    EXPECT_EQ(OB_SUCCESS, task_mgr_.init());
  }
  virtual void TearDown() {
  }
protected:
  ObLSRestoreTaskMgr task_mgr_;
};
// test the base function of ObLSRestoreTaskMgr
TEST_F(TestRestoreTaskMgr, taskMgr)
{
  LOG_INFO("TestRestoreTaskMgr::taskMgr begin");
  int64_t max_tablet_num = 2048;
  ObArray<ObTabletID> wait_tablet_ids, schedule_tablet_ids;
  int i = 1;
  while (i <= 2048) {
    wait_tablet_ids.push_back(ObTabletID(i++));
  }
  while(i <=4096) {
    schedule_tablet_ids.push_back(ObTabletID(i++));
  }

// add tablet in wait set or schedule set
  ASSERT_EQ(OB_SUCCESS, task_mgr_.add_tablet_in_wait_set(wait_tablet_ids));
  ASSERT_EQ(OB_SUCCESS, task_mgr_.add_tablet_in_schedule_set(schedule_tablet_ids));
  ASSERT_EQ(2048, task_mgr_.wait_tablet_set_.size());
  ASSERT_EQ(2048, task_mgr_.schedule_tablet_set_.size());
  ASSERT_EQ(0, task_mgr_.tablet_map_.size());

  ASSERT_EQ(OB_SUCCESS, task_mgr_.add_tablet_in_wait_set(wait_tablet_ids));
  ASSERT_EQ(2048, task_mgr_.wait_tablet_set_.size());

  task_mgr_.reuse_wait_set();
  ASSERT_EQ(0, task_mgr_.wait_tablet_set_.size());
  ASSERT_EQ(2048, task_mgr_.schedule_tablet_set_.size());

  ASSERT_EQ(OB_SUCCESS, task_mgr_.add_tablet_in_wait_set(schedule_tablet_ids));
  ASSERT_EQ(0, task_mgr_.wait_tablet_set_.size());
  ASSERT_EQ(2048, task_mgr_.schedule_tablet_set_.size());

  task_mgr_.reuse_set();
  ASSERT_EQ(0, task_mgr_.wait_tablet_set_.size());
  ASSERT_EQ(0, task_mgr_.schedule_tablet_set_.size());
  ASSERT_EQ(0, task_mgr_.tablet_map_.size());

  ObArray<ObTabletID> max_tablet_ids;
  i = 1;
  while (i <= (max_tablet_num)) {
    max_tablet_ids.push_back(ObTabletID(i++));
  }

  ASSERT_EQ(OB_SUCCESS, task_mgr_.add_tablet_in_wait_set(max_tablet_ids));
  ASSERT_EQ(max_tablet_num, task_mgr_.wait_tablet_set_.size());

// pop need restore tablets
  FakeLS ls(1001);
  ObSArray<ObTabletID> tablets_need_to_restore;
  ObSArray<ObTabletID> tablets_restored;
  ASSERT_EQ(OB_SUCCESS, task_mgr_.pop_need_restore_tablets(ls, tablets_need_to_restore));
  ASSERT_EQ(OB_SUCCESS, task_mgr_.pop_restored_tablets(ls, tablets_restored));
  ASSERT_EQ(1024, tablets_need_to_restore.count());
  ASSERT_EQ(0, tablets_restored.count());

// schedule restore tabelts
  share::ObTaskId task_id;
  bool reach_dag_limit = false;
  task_id.init(A);
  ASSERT_EQ(OB_SUCCESS, task_mgr_.schedule_tablet(task_id, tablets_need_to_restore, reach_dag_limit));
  ASSERT_EQ(1024, task_mgr_.wait_tablet_set_.size());
  ASSERT_EQ(1024, task_mgr_.schedule_tablet_set_.size());
  ASSERT_EQ(1, task_mgr_.tablet_map_.size());

// pop restored tablets
  global_restored = true;
  ASSERT_EQ(OB_SUCCESS, task_mgr_.pop_restored_tablets(ls, tablets_restored));
  ASSERT_EQ(1024, task_mgr_.wait_tablet_set_.size());
  ASSERT_EQ(1024, task_mgr_.schedule_tablet_set_.size());
  ASSERT_EQ(0, task_mgr_.tablet_map_.size());
  LOG_INFO("TestRestoreTaskMgr::taskMgr finish");
}


class TestLSRestoreHandler : public ::testing::Test
{
public:
  TestLSRestoreHandler() = default;
  virtual ~TestLSRestoreHandler() = default;
  static void SetUpTestCase()
  {
    ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
  static void create_ls()
  {
    ObCreateLSArg arg;
    uint64_t tenant_id = MTL_ID();
    ObLSService* ls_svr = MTL(ObLSService*);
    bool exist = false;
    ObLSHandle handle;
    ObLSRestoreStatus restore_status;
    ObLSID id_100(100);
    EXPECT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_100, arg));
    arg.tenant_info_.tenant_role_ = share::RESTORE_TENANT_ROLE;
    EXPECT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(id_100, handle, ObLSGetMod::STORAGE_MOD));
    EXPECT_EQ(OB_SUCCESS, handle.get_ls()->get_restore_status(restore_status));
    EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_START, restore_status);
  }

  void get_ls(ObLS *&ls)
  {
    int ret = OB_SUCCESS;
    ls = nullptr;
    ObLSHandle handle;
    ObLSID id_100(100);
    ObLSService* ls_svr = MTL(ObLSService*);
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(id_100, handle, ObLSGetMod::STORAGE_MOD));
    ls = handle.get_ls();
  }
private:
  ObLSRestoreHandler *restore_handler;
};




// test switch state
TEST_F(TestLSRestoreHandler, switch_state)
{
  LOG_INFO("TestLSRestoreHandler::switch_state begin");
  create_ls();
  ObLS *ls = nullptr;
  get_ls(ls);
  ObLSRestoreHandler *handler = ls->get_ls_restore_handler();
  const int64_t rebuild_seq = ls->get_rebuild_seq();
  LOG_INFO("try to switch state");
  EXPECT_EQ(true, nullptr == handler->state_handler_);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(true, nullptr != handler->state_handler_);
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_START, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::RESTORE_TABLETS_META), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_TABLETS_META, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::QUICK_RESTORE), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::QUICK_RESTORE, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA, handler->state_handler_->ls_restore_status_);

  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::RESTORE_START), rebuild_seq);
  ls->disable_replay();
  LOG_INFO("TestLSRestoreHandler::switch_state finish");
}
}

static bool ls_create = false;
static bool restore_finished = false;
static int check_ls_meta_exist_ret = true;

// ls not create, sys restore finished. move status to  restore none
namespace storage
{

int ObLSRestoreStartState::check_ls_leader_ready_(bool &is_ready) {
  is_ready = true;
  return OB_SUCCESS;
}

int ObLSRestoreStartState::check_ls_created_(bool &is_created) {
  is_created = ls_create;
  return OB_SUCCESS;
}
int ObLSRestoreStartState::check_sys_ls_restore_finished_(bool &restore_finish) {
  restore_finish = restore_finished;
  return OB_SUCCESS;
}

int ObLSRestoreStartState::check_ls_meta_exist_(bool &is_exist) {
  is_exist = check_ls_meta_exist_ret;
  return OB_SUCCESS;
}

}

namespace unittest
{
// ls not created and sys ls has restore finished
TEST_F(TestLSRestoreHandler, restore_start_1)
{
  LOG_INFO("TestLSRestoreHandler::restore_start_1 begin");
  ls_create = false;
  restore_finished = true;
  ObLS *ls = nullptr;
  get_ls(ls);
  const int64_t rebuild_seq = ls->get_rebuild_seq();
  ObLSRestoreStatus status(ObLSRestoreStatus::Status::RESTORE_START);
  ls->set_restore_status(status, rebuild_seq);
  ObLSRestoreHandler *handler = ls->get_ls_restore_handler();
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_START, handler->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, handler->state_handler_->do_restore());
  EXPECT_EQ(OB_SUCCESS, ls->get_restore_status(status));
  EXPECT_EQ(ObLSRestoreStatus::Status::NONE, status);
  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::RESTORE_START), rebuild_seq);
  ls->disable_replay();
  LOG_INFO("TestLSRestoreHandler::restore_start_1 finish");
}
// ls created, and ls meta not exist
TEST_F(TestLSRestoreHandler, restore_start_2)
{
  LOG_INFO("TestLSRestoreHandler::restore_start_2 begin");
  ls_create = true;
  check_ls_meta_exist_ret = false;
  ObLS *ls = nullptr;
  get_ls(ls);
  const int64_t rebuild_seq = ls->get_rebuild_seq();
  ObLSRestoreStatus status(ObLSRestoreStatus::Status::RESTORE_START);
  ObLSRestoreHandler *handler = ls->get_ls_restore_handler();
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_START, handler->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, handler->state_handler_->do_restore());
  EXPECT_EQ(OB_SUCCESS, ls->get_restore_status(status));
  EXPECT_EQ(ObLSRestoreStatus::Status::QUICK_RESTORE, status);
  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::RESTORE_START), rebuild_seq);
  ls->disable_replay();
  LOG_INFO("TestLSRestoreHandler::restore_start_2 finish");
}
// ls created and ls meta exist
TEST_F(TestLSRestoreHandler, restore_start_3)
{
  LOG_INFO("TestLSRestoreHandler::restore_start_3 begin");
  ls_create = true;
  check_ls_meta_exist_ret = true;
  ObLS *ls = nullptr;
  get_ls(ls);
  ObLSRestoreStatus status(ObLSRestoreStatus::Status::RESTORE_START);
  ObLSRestoreHandler *handler = ls->get_ls_restore_handler();
  EXPECT_EQ(OB_SUCCESS, handler->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_START, handler->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, handler->state_handler_->do_restore());
  EXPECT_EQ(OB_SUCCESS, ls->get_restore_status(status));
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS, status);
  LOG_INFO("TestLSRestoreHandler::restore_start_3 finish");
}
}

ObRole ob_role;
namespace logservice
{
int ObLogService::get_palf_role(const share::ObLSID &id, common::ObRole &role,
    int64_t &proposal_id)
{
  UNUSED(id);
  role = ob_role;
  proposal_id = 1;
  return OB_SUCCESS;
}
}

static int schedule_ls_restore_ret = OB_SUCCESS;

namespace storage
{
int ObILSRestoreState::schedule_ls_restore_(const ObLSRestoreArg &arg,
    const share::ObTaskId &task_id)
{
  UNUSEDx(arg, task_id);
  return schedule_ls_restore_ret;
}

static ObLSRestoreStatus leader_status(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS);

int ObILSRestoreState::request_leader_status_(ObLSRestoreStatus &leader_restore_status)
{
  leader_restore_status = leader_status;
  return OB_SUCCESS;
}

int ObILSRestoreState::check_all_follower_restore_finish_(bool &finish)
{
  finish = true;
  return OB_SUCCESS;
}

int ObLSRestoreSysTabletState::follower_fill_ls_restore_arg_(ObLSRestoreArg &arg)
{
  int ret = OB_SUCCESS;
  arg.tenant_id_ = ls_restore_arg_->get_tenant_id();
  arg.ls_id_ = ls_->get_ls_id();
  arg.is_leader_ = false;
  return ret;
}

int ObLSRestoreHandler::handle_execute_over(
    const share::ObTaskId &task_id,
    const ObIArray<common::ObTabletID> &restore_succeed_tablets,
    const ObIArray<common::ObTabletID> &restore_failed_tablets,
    const share::ObLSID &ls_id,
    const int &result)
{
  int ret = OB_SUCCESS;
  UNUSEDx(task_id, restore_succeed_tablets, restore_failed_tablets, ls_id, result);
  state_handler_->set_retry_flag();
  return ret;
}

}

namespace unittest
{
// leader restore sys and follower restore sys
TEST_F(TestLSRestoreHandler, restore_sys)
{
  LOG_INFO("TestLSRestoreHandler::restore_sys begin");
  ObLS *ls = nullptr;
  get_ls(ls);
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS;
  const int64_t rebuild_seq = ls->get_rebuild_seq();
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);

  ob_role = ObRole::LEADER;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  ObLSRestoreTaskMgr &tablet_mgr = ls->get_ls_restore_handler()->state_handler_->tablet_mgr_;
  EXPECT_EQ(1, tablet_mgr.tablet_map_.size());
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(0, tablet_mgr.tablet_map_.size());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS, ls->ls_meta_.restore_status_);

  ob_role = ObRole::FOLLOWER;
  ls->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS), rebuild_seq);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(1, tablet_mgr.tablet_map_.size());
  share::ObTaskId task_id;
  ObArray<common::ObTabletID> restore_succeed_tablets;
  ObArray<common::ObTabletID> restore_failed_tablets;
  share::ObLSID ls_id;
  int result;
  ls->get_ls_restore_handler()->handle_execute_over(task_id, restore_succeed_tablets, restore_failed_tablets, ls_id, result);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(0, tablet_mgr.tablet_map_.size());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS, ls->ls_meta_.restore_status_);
  LOG_INFO("TestLSRestoreHandler::restore_sys finish");
}
}

namespace storage
{

int ObLSTabletService::build_tablet_iter(ObLSTabletIterator &iter, const bool except_ls_inner_tablet)
{
  UNUSED(iter);
  UNUSED(except_ls_inner_tablet);
  return OB_SUCCESS;
}

int ObLSTabletIterator::get_next_tablet(ObTabletHandle &handle)
{
  UNUSED(handle);
  return OB_ITER_END;
}

}

namespace unittest
{
// quick restore , restore tablet meta, and restore major has the same flow
// just test in one case.
TEST_F(TestLSRestoreHandler, restore_group_restore)
{
  LOG_INFO("TestLSRestoreHandler::restore_group_restore begin");
  ObLS *ls = nullptr;
  get_ls(ls);
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::RESTORE_TABLETS_META;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_TABLETS_META, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);

  ob_role = ObRole::LEADER;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META, ls->ls_meta_.restore_status_);
  LOG_INFO("TestLSRestoreHandler::restore_group_restore finish");
}

TEST_F(TestLSRestoreHandler, wait_state)
{
  LOG_INFO("TestLSRestoreHandler::wait_state begin");
  ObLS *ls = nullptr;
  get_ls(ls);
  ob_role = ObRole::LEADER;

  // leader in wait restore sys tablets
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_TABLETS_META, ls->ls_meta_.restore_status_);
  // leader in wait restore meta tablets
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN, ls->ls_meta_.restore_status_);
  // leader in wait quick restore
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH, ls->ls_meta_.restore_status_);
  // leader in quick restore finish
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA, ls->ls_meta_.restore_status_);
  // leader in wait restore major
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::NONE, ls->ls_meta_.restore_status_);

  ob_role = ObRole::FOLLOWER;
  // follower in wait restore sys tablets
  leader_status = ObLSRestoreStatus::Status::RESTORE_TABLETS_META;
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_TABLETS_META, ls->ls_meta_.restore_status_);

  leader_status = ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_TABLETS_META, ls->ls_meta_.restore_status_);

  leader_status = ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS, ls->ls_meta_.restore_status_);

  // follower in wait restore meta tablets
  leader_status = ObLSRestoreStatus::Status::QUICK_RESTORE;
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN, ls->ls_meta_.restore_status_);

  leader_status = ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN, ls->ls_meta_.restore_status_);

  leader_status = ObLSRestoreStatus::Status::RESTORE_TABLETS_META;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META, ls->ls_meta_.restore_status_);

  // follower in wait quick restore
  leader_status = ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH;
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH, ls->ls_meta_.restore_status_);

  leader_status = ObLSRestoreStatus::Status::QUICK_RESTORE;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE, ls->ls_meta_.restore_status_);

  // follower in quick restore finish
  leader_status = ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA;
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA, ls->ls_meta_.restore_status_);

  leader_status = ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH, ls->ls_meta_.restore_status_);

  // follower in wait restore major data
  leader_status = ObLSRestoreStatus::Status::NONE;
  ls->get_ls_restore_handler()->state_handler_ = nullptr;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->update_state_handle_());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA, ls->get_ls_restore_handler()->state_handler_->ls_restore_status_);
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::NONE, ls->ls_meta_.restore_status_);

  leader_status = ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA;
  ls->ls_meta_.restore_status_ = ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA;
  EXPECT_EQ(OB_SUCCESS, ls->get_ls_restore_handler()->state_handler_->do_restore());
  EXPECT_EQ(ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA, ls->ls_meta_.restore_status_);

  ObLSService *ls_svr = MTL(ObLSService*);
  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(ObLSID(100)));
  LOG_INFO("TestLSRestoreHandler::wait_state finish");
}

}
}

int main(int argc, char *argv[])
{
  system("rm -f ./test_ls_restore.log*");
  OB_LOGGER.set_file_name("test_ls_restore.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
