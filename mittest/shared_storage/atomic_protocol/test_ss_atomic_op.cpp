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
#define USING_LOG_PREFIX STORAGETEST

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_default_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_overwrite_op.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_define.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_op.h"
#include "lib/string/ob_string_holder.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "shared_storage/clean_residual_data.h"
#include "test_ss_atomic_util.h"
#include "storage/test_tablet_helper.h"
#include "storage/init_basic_struct.h"
#include "storage/tx_storage/ob_ls_service.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
typedef ObDefaultSSMetaSSLogValue<ObSSLSMeta> ObSSLSMetaSSLogValue;
typedef ObAtomicDefaultFile<ObSSLSMetaSSLogValue> ObAtomicLSMetaFile;

static int64_t lease_epoch = 1;

using namespace common;

bool is_file_use_sslog(const ObAtomicFileType type, const ObLSID &ls_id)
{
  return false;
}

bool is_meta_use_sslog(const sslog::ObSSLogMetaType type, const ObLSID &ls_id)
{
  return false;
}

static bool global_is_sswriter = true;
void mock_switch_sswriter()
{
  ATOMIC_INC(&lease_epoch);
  LOG_INFO("mock switch sswriter", K(lease_epoch));
}

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  is_sswriter = global_is_sswriter;
  epoch = ATOMIC_LOAD(&lease_epoch);
  return OB_SUCCESS;
}

int ObTablet::check_meta_addr() const
{
  int ret = OB_SUCCESS;
  return ret;
}

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

class TestAtomicOp : public ::testing::Test
{
public:
  TestAtomicOp() = default;
  virtual ~TestAtomicOp() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  static int tablet_cur_op_id;
};

int TestAtomicOp::tablet_cur_op_id = 0;

void TestAtomicOp::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestAtomicOp::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

ObSSTableInfoList write_ss_list;

TEST_F(TestAtomicOp, test_sstable_list_add_op)
{
  int ret = OB_SUCCESS;
  ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
  ASSERT_NE(nullptr, atomic_file_mgr);
  GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1);
  ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, sstablelist_file);
  GET_ONE_SSTABLE_INFO(write_info1, 2);
  GENERATE_ADD_SSTABEL_TASK_INFO(task_info1, write_info1)
  {
    // test write sstable list and read sstable list when preceding sstable list is empty
    // create op
    CREATE_SSTABLE_LIST_ADD_OP_WITH_RECONFIRM(op_handle);
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    write_ss_list.push_back(write_info1);
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info1));
    // finish op and flush buffer to share storage, generate sstable list obj
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));
    // read sstable list
    ObSSTableInfoList read_ss_list;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));

    ObSSTableTaskInfo read_task_info1;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_task(2, read_task_info1));
    // data seq and meta seq should be changed succssfully
    ASSERT_EQ(true, task_info1 == read_task_info1);
  }

  {
    // test write sstable list and read sstable list when preceding sstable list is empty
    // create op
    CREATE_SSTABLE_LIST_ADD_OP(op_handle);
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    // write sstable task obj and sstable list obj
    GET_ONE_SSTABLE_INFO(write_info2, op_id);
    GENERATE_ADD_SSTABEL_TASK_INFO(task_info2, write_info2)
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info2));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->abort_op(op_handle));
    // read sstable list
    ObSSTableInfoList read_ss_list;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }

  {
    // test upload multiple sstables
    CREATE_SSTABLE_LIST_ADD_OP(op_handle);
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    // write sstable task obj and sstable list obj
    GET_ONE_SSTABLE_INFO(write_info2, op_id);
    GENERATE_ADD_SSTABEL_TASK_INFO(task_info2, write_info2)
    GET_ONE_SSTABLE_INFO(write_info3, op_id);
    GENERATE_ADD_SSTABEL_TASK_INFO(task_info3, write_info3)
    ObSSTableTaskInfoList task_list;
    task_list.push_back(task_info2);
    task_list.push_back(task_info3);
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_list(task_list));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));
    write_ss_list.push_back(write_info2);
    write_ss_list.push_back(write_info3);
    // read sstable list
    ObSSTableInfoList read_ss_list;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }

  {
    // test upload multiple sstables
    CREATE_SSTABLE_LIST_ADD_OP(op_handle);
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    // write sstable task obj and sstable list obj
    GET_ONE_SSTABLE_INFO(write_info2, op_id);
    GENERATE_ADD_SSTABEL_TASK_INFO(task_info2, write_info2)
    GET_ONE_SSTABLE_INFO(write_info3, op_id);
    GENERATE_ADD_SSTABEL_TASK_INFO(task_info3, write_info3)
    ObSSTableTaskInfoList task_list;
    task_list.push_back(task_info2);
    task_list.push_back(task_info3);
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_list(task_list));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->abort_op(op_handle));
    // read sstable list
    ObSSTableInfoList read_ss_list;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }
}

// TEST_F(TestAtomicOp, test_sstable_list_remove_op)
// {
//   int ret = OB_SUCCESS;
//   ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
//   ASSERT_NE(nullptr, atomic_file_mgr);
//   GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1);
//   ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
//   ASSERT_NE(nullptr, sstablelist_file);
//   GET_ONE_SSTABLE_INFO(write_info1, tablet_cur_op_id);
//   GENERATE_ADD_SSTABEL_TASK_INFO(task_info1, write_info1)
//   {
//     CREATE_SSTABLE_LIST_ADD_OP(op_handle);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     write_ss_list.push_back(write_info1);
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info1));
//     // finish op and flush buffer to share storage, generate sstable list obj
//     ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));
//   }

//   GET_ONE_SSTABLE_INFO(write_info2, 6);
//   GENERATE_ADD_SSTABEL_TASK_INFO(task_info2, write_info2);
//   {
//     CREATE_SSTABLE_LIST_ADD_OP(op_handle);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     write_ss_list.push_back(write_info2);
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info2));
//     // finish op and flush buffer to share storage, generate sstable list obj
//     ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));
//   }
// }

// TEST_F(TestAtomicOp, test_sstable_list_replace_op)
// {
//   int ret = OB_SUCCESS;
//   ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
//   ASSERT_NE(nullptr, atomic_file_mgr);
//   GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1);
//   ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
//   ASSERT_NE(nullptr, sstablelist_file);
//   GET_ONE_SSTABLE_INFO(write_info1, tablet_cur_op_id);
//   GENERATE_ADD_SSTABEL_TASK_INFO(task_info1, write_info1)
//   {
//     CREATE_SSTABLE_LIST_ADD_OP(op_handle);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     write_ss_list.push_back(write_info1);
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info1));
//     // finish op and flush buffer to share storage, generate sstable list obj
//     ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));
//   }

//   GET_ONE_SSTABLE_INFO(write_info2, 6);
//   GENERATE_ADD_SSTABEL_TASK_INFO(task_info2, write_info2);
//   {
//     CREATE_SSTABLE_LIST_ADD_OP(op_handle);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     write_ss_list.push_back(write_info2);
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info2));
//     // finish op and flush buffer to share storage, generate sstable list obj
//     ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));
//   }
// }

// TEST_F(TestAtomicOp, test_file_too_large)
// {
//   int ret = OB_SUCCESS;
//   ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
//   ASSERT_NE(nullptr, atomic_file_mgr);
//   GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1);
//   ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
//   ASSERT_NE(nullptr, sstablelist_file);

//   {
//     GET_ONE_SSTABLE_INFO(write_info1, tablet_cur_op_id);
//     GENERATE_ADD_SSTABEL_TASK_INFO(task_info1, write_info1)
//     char *buf = new char[2 * 1024 * 1024]; // 2MB
//     task_info1.storage_schema_buf_.assign(ObString(2*1024*1024, buf));
//     ASSERT_EQ(true, task_info1.get_serialize_size() > MAX_MINI_SSTABLE_LIST_TASK_OBJ_SIZE);
//     CREATE_SSTABLE_LIST_ADD_OP(op_handle);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     write_ss_list.push_back(write_info1);
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info1));
//     // finish op and flush buffer to share storage, generate sstable list obj
//     ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));

//     ObSSTableTaskInfo read_task_file;
//     ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_task(op_id, read_task_file));
//     LOG_INFO("log debug", K(read_task_file), K(task_info1));
//     ASSERT_EQ(true, read_task_file == task_info1);
//     delete[] buf;
//   }

//   {
//     GET_ONE_SSTABLE_INFO(write_info1, tablet_cur_op_id);
//     GENERATE_ADD_SSTABEL_TASK_INFO(task_info1, write_info1)
//     char *buf = new char[11 * 1024 * 1024]; // 11MB
//     task_info1.storage_schema_buf_.assign(ObString(11*1024*1024, buf));
//     ASSERT_EQ(true, task_info1.get_serialize_size() > MAX_SSTABLE_TASK_OBJ_READ_SIZE);
//     CREATE_SSTABLE_LIST_ADD_OP(op_handle);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     write_ss_list.push_back(write_info1);
//     ASSERT_EQ(OB_NOT_SUPPORTED, op_handle.get_atomic_op()->write_add_task_info(task_info1));
//     // finish op and flush buffer to share storage, generate sstable list obj
//     ASSERT_EQ(OB_SUCCESS, sstablelist_file->abort_op(op_handle));
//   }

//   {
//     ObSSTableInfoList sstable_info_list;
//     ASSERT_EQ(OB_SUCCESS, sstable_info_list.op_list_.reserve(500 * 1024)); // larger than 1MB
//     for (int i = 0; i < 500 * 1024; i++) {
//       sstable_info_list.op_list_.push_back(ObSStableInfo());
//     }
//     ASSERT_EQ(true, sstable_info_list.get_serialize_size() > SSTABLE_LIST_OBJ_READ_SIZE);
//     ObSSTableInfoList read_info_list;
//     ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_info_list));
//   }

//   {
//     ObSSTableInfoList sstable_info_list;
//     ASSERT_EQ(OB_SUCCESS, sstable_info_list.op_list_.reserve(1000 * 1024)); // larger than 10MB
//     for (int i = 0; i < 500 * 1024; i++) {
//       sstable_info_list.op_list_.push_back(ObSStableInfo());
//     }
//     ASSERT_EQ(true, sstable_info_list.get_serialize_size() > MAX_SSTABLE_LIST_OBJ_READ_SIZE);
//     ASSERT_EQ(OB_NOT_SUPPORTED, sstablelist_file->write_sstable_list(sstable_info_list));
//   }
// }

// TEST_F(TestAtomicOp, test_ls_meta_write_op)
// {
//   int ret = OB_SUCCESS;
//   uint64_t tenant_id = MTL_ID();
//   ObLSService *ls_svr = MTL(ObLSService*);
//   ObCreateLSArg arg;
//   ObLSHandle handle;
//   ObLS *ls = NULL;
//   ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
//   ASSERT_NE(nullptr, atomic_file_mgr);
//   tablet_cur_op_id = 0;
//   const int64_t LS_ID = 1111;
//   ObLSID ls_id(LS_ID);
//   // create ls
//   ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
//   ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
//   EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
//   ls = handle.get_ls();
//   ASSERT_NE(nullptr, ls);

//   ObMemberList member_list;
//   (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
//   GlobalLearnerList learner_list;
//   int64_t paxos_replica_num = 1;
//   ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(member_list,
//                                                     paxos_replica_num,
//                                                     learner_list));

//   for (int i=0;i<15;i++) {
//     ObRole role;
//     int64_t proposal_id = 0;
//     ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
//     if (role == ObRole::LEADER) {
//       break;
//     }
//     ::sleep(1);
//   }

//   {
//     LOG_INFO("write ls meta with sswriter");
//     GET_LS_META_HANLDE(file_handle, 2, 1);
//     ObAtomicLSMetaFile *ls_meta_file = file_handle1.get_atomic_file();
//     ASSERT_NE(nullptr, ls_meta_file);
//     // test write ls meta and read ls meta
//     // create op
//     CREATE_LS_META_WRITE_OP_WITH_RECONFIRM(op_handle, true);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     ObLSMeta ls_meta = ls->get_ls_meta();
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_task_info(ls_meta));
//     // finish op and flush buffer to share storage
//     ASSERT_EQ(OB_SUCCESS, ls_meta_file->finish_op(op_handle));
//     // read ls meta
//     ObSSLSMeta read_ls_meta;
//     LOG_INFO("read ls meta");
//     ASSERT_EQ(OB_SUCCESS, ls_meta_file->read_file_info(read_ls_meta));
//     ASSERT_EQ(ls_id, read_ls_meta.ls_id_);
//   }

//   {
//     LOG_INFO("write ls meta without sswriter");
//     tablet_cur_op_id = 0;
//     GET_LS_META_HANLDE(file_handle, 3, 1);
//     ObAtomicLSMetaFile *ls_meta_file = file_handle1.get_atomic_file();
//     ASSERT_NE(nullptr, ls_meta_file);
//     // op_id=0 no sswriter
//     global_is_sswriter = false;
//     CREATE_LS_META_WRITE_OP_WITH_RECONFIRM(op_handle, false);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     ObLSMeta ls_meta = ls->get_ls_meta();
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_task_info(ls_meta));
//     // finish op and flush buffer to share storage

//     ASSERT_EQ(OB_SUCCESS, ls_meta_file->finish_op(op_handle));
//     // read sstable list
//     ObSSLSMeta read_ls_meta;
//     LOG_INFO("read ls meta");
//     ASSERT_EQ(OB_SUCCESS, ls_meta_file->read_file_info(read_ls_meta));
//     ASSERT_EQ(ls_id, read_ls_meta.ls_id_);
//     ObSSLSMeta read_ls_meta_task_info;
//     // ASSERT_EQ(OB_SUCCESS, ls_meta_file->get_task_info(0, read_ls_meta_task_info));
//     // ASSERT_EQ(ls_id, read_ls_meta_task_info.ls_id_);
//     global_is_sswriter = true;
//   }
// }

// TEST_F(TestAtomicOp, test_tablet_meta_write_op)
// {
//   int ret = OB_SUCCESS;

//   LOG_INFO("test_tablet_meta_write_op");
//   uint64_t tenant_id = MTL_ID();
//   ObLSService *ls_svr = MTL(ObLSService*);
//   ObCreateLSArg arg;
//   ObLSHandle handle;
//   ObLS *ls = NULL;
//   ObTabletHandle tablet_handle;
//   ObTablet *tablet =NULL;

//   ObAtomicFileMgr* atomic_file_mgr = MTL(ObAtomicFileMgr*);
//   ASSERT_NE(nullptr, atomic_file_mgr);
//   const int64_t TABLET_ID = 200001;
//   const int64_t LS_ID = 1010;
//   ObTabletID tablet_id(TABLET_ID);
//   ObLSID ls_id(LS_ID);

//   // create ls
//   ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
//   ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
//   EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
//   ls = handle.get_ls();
//   ASSERT_NE(nullptr, ls);

//   ObMemberList member_list;
//   (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
//   GlobalLearnerList learner_list;
//   int64_t paxos_replica_num = 1;
//   ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(member_list,
//                                                     paxos_replica_num,
//                                                     learner_list));

//   for (int i=0;i<15;i++) {
//     ObRole role;
//     int64_t proposal_id = 0;
//     ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
//     if (role == ObRole::LEADER) {
//       break;
//     }
//     ::sleep(1);
//   }

//   common::ObArenaAllocator allocator("TestTabletMeta",
//                                      OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
//   // create a tablet
//   share::schema::ObTableSchema table_schema;
//   uint64_t table_id = 12345;
//   ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
//   ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(handle, tablet_id, table_schema, allocator));

//   ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
//   tablet = tablet_handle.get_obj();
//   ASSERT_NE(nullptr, tablet);
//   ASSERT_EQ(tablet_id, tablet->get_tablet_meta().tablet_id_);

//   GET_TABLET_META_HANLDE_DEFAULT(file_handle, 1, LS_ID, TABLET_ID);
//   ObAtomicTabletMetaFile *tablet_meta_file = file_handle1.get_atomic_file();
//   ASSERT_NE(nullptr, tablet_meta_file);
//   tablet_cur_op_id = 0;
//   {
//     // test write tablet meta and read tablet meta when preceding tablet meta is empty
//     // create op
//     CREATE_TABLET_META_WRITE_OP_WITH_RECONFIRM(op_handle);
//     uint64_t op_id = 0;
//     ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
//     ASSERT_EQ(tablet_cur_op_id, op_id);
//     // write tablet meta
//     ObTabletTaskFileInfo task_info;

//     task_info.type_ = ObAtomicOpType::TABLET_META_WRITE_OP;
//     task_info.set_tablet(tablet);
//     ObSSMetaUpdateMetaInfo meta_info;
//     meta_info.set(ObMetaUpdateReason::CREATE_TABLET, tablet->get_tablet_meta().get_acquire_scn());
//     task_info.set_meta_info(meta_info);

//     ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_task_info(task_info));
//     // finish op and flush buffer to share storage, generate ls meta obj
//     ASSERT_EQ(OB_SUCCESS, tablet_meta_file->finish_op(op_handle));
//     // read tablet meta
//     tablet = nullptr;
//     common::ObArenaAllocator allocator("TestTabletMeta",
//                                        OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
//     ASSERT_EQ(OB_SUCCESS, tablet_meta_file->get_tablet(allocator, tablet));
//     ASSERT_EQ(tablet_id, tablet->get_tablet_id());
//   }
// }


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_atomic_op.log*");
  OB_LOGGER.set_file_name("test_atomic_op.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
