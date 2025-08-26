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
#include "storage/incremental/atomic_protocol/ob_atomic_file_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_op.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "shared_storage/clean_residual_data.h"
#include "test_ss_atomic_util.h"
#include "mock_object_storage.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObSSTableInfoList write_ss_list;

bool is_file_use_sslog(const ObAtomicFileType type, const ObLSID &ls_id)
{
  return false;
}

bool is_meta_use_sslog(const sslog::ObSSLogMetaType type, const ObLSID &ls_id)
{
  return false;
}

class TestAtomicFile : public ::testing::Test
{
public:
  TestAtomicFile() = default;
  virtual ~TestAtomicFile() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  static int tablet_cur_op_id;
};

int TestAtomicFile::tablet_cur_op_id = 0;


void TestAtomicFile::SetUpTestCase()
{
  ObAtomicFile::WRITE_FILE_TIMEOUT_MAX_RETRY = 0;
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  OB_MOCK_OBJ_SRORAGE.tenant_ = ObTenantEnv::get_tenant();
  MTL(ObTenantDagScheduler*)->stop();
}

void TestAtomicFile::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  OB_MOCK_OBJ_SRORAGE.stop();
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestAtomicFile, test_file_read_write)
{
  int ret = OB_SUCCESS;
  GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1)
  ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, sstablelist_file);

  int64_t pos = 0;
  char write_buf[MAX_BUF_SIZE];
  char read_buf[MAX_BUF_SIZE];
  GENERATE_OP_ID(op_id1)
  ASSERT_EQ(op_id1, tablet_cur_op_id);
  GET_ONE_SSTABLE_INFO(write_info1, op_id1)
  {
    // test read-write
    GENERATE_SSTABLE_LIST_FILE_OPT(op_id1, opt1)

    // test write interface
    ObFileBaseObj file_base_obj;
    ASSERT_EQ(OB_SUCCESS, file_base_obj.serialize(write_buf, sizeof(write_buf), pos));
    ASSERT_EQ(OB_SUCCESS, write_ss_list.serialize(write_buf, sizeof(write_buf), pos));
    ObAtomicFileBuffer buffer(write_buf, pos);
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->write(opt1, buffer));

    // test read interface
    MacroBlockId block_id;
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt1, block_id));
    ObAtomicFileBuffer read_buffer(read_buf, sizeof(read_buf));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->read(block_id, read_buffer));
    ObSSTableInfoList read_info1;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, read_info1.deserialize(read_buffer.buf_+file_base_obj.get_serialize_size(), read_buffer.size_, pos));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_info1));
  }

  GENERATE_OP_ID(op_id2);
  ASSERT_EQ(op_id2, tablet_cur_op_id);
  {
    // test read lattest file content
    GENERATE_SSTABLE_LIST_FILE_OPT(op_id2, opt2)
    GET_ONE_SSTABLE_INFO(write_info2, op_id2)
    write_ss_list.push_back(write_info2);
    write_ss_list.op_id_ = tablet_cur_op_id;
    write_ss_list.op_type_ = ObAtomicOpType::SS_LIST_ADD_OP;
    pos = 0;
    ObFileBaseObj file_base_obj;
    ASSERT_EQ(OB_SUCCESS, file_base_obj.serialize(write_buf, sizeof(write_buf), pos));
    ASSERT_EQ(OB_SUCCESS, write_ss_list.serialize(write_buf, sizeof(write_buf), pos));
    ObAtomicFileBuffer buffer2(write_buf, pos);
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->write(opt2, buffer2));
    ObAtomicFileBuffer read_buffer2(read_buf, sizeof(read_buf));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->read_lattest_file_content_(read_buffer2));
    ObSSTableInfoList read_info2;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, read_info2.deserialize(read_buffer2.buf_+file_base_obj.get_serialize_size(), read_buffer2.size_, pos));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_info2));
  }

  {
    // test read file content with op id
    ObFileBaseObj file_base_obj;
    ObAtomicFileBuffer read_buffer3(read_buf, sizeof(read_buf));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->read_file_content_(op_id1, read_buffer3));
    ObSSTableInfoList read_info3;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, read_info3.deserialize(read_buffer3.buf_+file_base_obj.get_serialize_size(), read_buffer3.size_, pos));
    ASSERT_EQ(true, is_array_prefix_same(write_ss_list, read_info3));
  }

  {
    // test get current op id obj
    uint64_t cur_op_id = 0;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_current_op_id_(cur_op_id));
    ASSERT_EQ(op_id2, cur_op_id);
  }

  {
    // test get next op id obj
    uint64_t next_op_id = 0;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_next_op_id_(next_op_id));
    ASSERT_EQ(op_id2+1, next_op_id);
  }

  {
    // test generate op id
    ObStorageObjectOpt opt4;
    tablet_cur_op_id = op_id2 + 1;
    int64_t cur_us1 = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->generate_op_id_obj_(tablet_cur_op_id));
    sstablelist_file->current_op_id_ = tablet_cur_op_id;

    int64_t cur_us2 = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->generate_op_id_obj_opt_(tablet_cur_op_id, opt4));

    MacroBlockId block_id;
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt4, block_id));
    ObAtomicFileBuffer read_buffer4(read_buf, sizeof(read_buf));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->read(block_id, read_buffer4));
    ObOpIdObj current_op_id_obj;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, current_op_id_obj.deserialize(read_buffer4.buf_, read_buffer4.size_, pos));
    ASSERT_EQ(true, current_op_id_obj.create_ts_ <= cur_us2 && current_op_id_obj.create_ts_ >= cur_us1);
  }

  {
    // test condition write
  }
}

TEST_F(TestAtomicFile, test_file_op_operation)
{
  int ret = OB_SUCCESS;
  GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1)
  ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, sstablelist_file);

  {
    // create op
    CREATE_SSTABLE_LIST_ADD_OP_WITH_RECONFIRM(op_handle)
    std::cout << "ssss1" << std::endl;
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    // finish op
    ASSERT_EQ(tablet_cur_op_id, op_id);
    // should write task info before finish op
    ASSERT_EQ(OB_ERR_UNEXPECTED, sstablelist_file->finish_op(op_handle));
    std::cout << "ssss2" << std::endl;
    // abort op
    CREATE_SSTABLE_LIST_ADD_OP(op_handle2)
    ASSERT_EQ(OB_SUCCESS, op_handle2.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->abort_op(op_handle2));
    ASSERT_EQ(nullptr, sstablelist_file->current_op_handle_.get_atomic_op());
    std::cout << "ssss3" << std::endl;
  }
}

TEST_F(TestAtomicFile, test_sstable_list_read_write)
{
  int ret = OB_SUCCESS;
  GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1)
  ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, sstablelist_file);
  char buf1[20] = "schema buf";
  char buf2[20] = "meta buf";
  GET_ONE_SSTABLE_INFO(write_info1, tablet_cur_op_id)
  GENERATE_ADD_SSTABEL_TASK_INFO(task_info1, write_info1)
  uint64_t read_op_id1 = 0;
  uint64_t read_op_id2 = 0;
  {
    // test write sstable list and read sstable list when preceding sstable list is empty
    // create op
    CREATE_SSTABLE_LIST_ADD_OP(op_handle)
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    read_op_id1 = tablet_cur_op_id;
    // write sstable task obj and sstable list obj
    write_ss_list.push_back(write_info1);
    write_ss_list.op_id_ = tablet_cur_op_id;
    write_ss_list.op_type_ = ObAtomicOpType::SS_LIST_ADD_OP;
    task_info1.storage_schema_buf_.assign(ObString(strlen(buf1), buf1));
    task_info1.sstable_meta_buf_.assign(ObString(strlen(buf2), buf2));
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info1));
    // finish op and flush buffer to share storage, generate sstable list obj
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));
    // read sstable list
    ObSSTableInfoList read_ss_list;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    LOG_INFO("debug log000", K(write_ss_list), K(read_ss_list));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }

  GET_ONE_SSTABLE_INFO(write_info2, tablet_cur_op_id)
  GENERATE_ADD_SSTABEL_TASK_INFO(task_info2, write_info2)
  {
    // test write sstable list and read sstable list when preceding sstable list is not empty
    // create op
    CREATE_SSTABLE_LIST_ADD_OP(op_handle)
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    read_op_id2 = tablet_cur_op_id;
    // write sstable task obj and sstable list obj
    write_ss_list.push_back(write_info2);
    write_ss_list.op_id_ = tablet_cur_op_id;
    write_ss_list.op_type_ = ObAtomicOpType::SS_LIST_ADD_OP;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info2));
    // finish op and flush buffer to share storage, generate sstable list obj
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));
    // read sstable list
    ObSSTableInfoList read_ss_list;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));

    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));

  }

  {
    // test get sstable task content by op id
    ObSSTableTaskInfo read_task_info1;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_task(read_op_id1, read_task_info1));
    ASSERT_EQ(true, read_task_info1 == task_info1);
    ObSSTableTaskInfo read_task_info2;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_task(read_op_id2, read_task_info2));
    ASSERT_EQ(true, read_task_info2 == task_info2);
  }
}

TEST_F(TestAtomicFile, test_sstable_list_sswriter_assume)
{
  MTL(ObTenantDagScheduler*)->stop();
  write_ss_list.reset();
  int ret = OB_SUCCESS;
  char write_buf[MAX_BUF_SIZE];
  char read_buf[MAX_BUF_SIZE];
  tablet_cur_op_id = 0;
  GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1, 200002, 1)
  ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, sstablelist_file);
  GET_ONE_SSTABLE_INFO(write_info1, 1)
  GENERATE_ADD_SSTABEL_TASK_INFO(task_info1, write_info1)
  {
    ObSSTableInfoList read_ss_list;
    ASSERT_EQ(OB_OBJECT_NOT_EXIST, sstablelist_file->get_sstable_list(read_ss_list));
  }

  {
    // test assume and generate sstablelist.op_id that sstable.op_id's finish is false
    // 1. finish op
    CREATE_SSTABLE_LIST_ADD_OP_WITH_RECONFIRM(op_handle);
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    uint64_t read_op_id1 = tablet_cur_op_id;
    // write sstable task obj and sstable list obj
    write_ss_list.push_back(write_info1);
    write_ss_list.op_id_ = tablet_cur_op_id;
    write_ss_list.op_type_ = ObAtomicOpType::SS_LIST_ADD_OP;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info1));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->finish_op(op_handle));

    // 2. abort op
    CREATE_SSTABLE_LIST_ADD_OP(op_handle2);
    uint64_t op_id2 = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle2.get_op_id(op_id2));
    ASSERT_EQ(tablet_cur_op_id, op_id2);
    // write sstable task obj and sstable list obj
    GET_ONE_SSTABLE_INFO(write_info2, op_id2);
    GENERATE_ADD_SSTABEL_TASK_INFO(task_info2, write_info2)
    ASSERT_EQ(OB_SUCCESS, op_handle2.get_atomic_op()->write_add_task_info(task_info2));
    // finish op and flush buffer to share storage, generate sstable list obj
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->abort_op(op_handle2));
    // recheck
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->on_sswriter_resume());
    tablet_cur_op_id += 1;
    // test get current op id obj
    uint64_t cur_op_id = 0;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_current_op_id_(cur_op_id));
    ASSERT_EQ(tablet_cur_op_id, cur_op_id);
    // test read file content with op id
    int64_t pos = 0;
    ObAtomicFileBuffer read_buffer3(read_buf, sizeof(read_buf));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->read_file_content_(read_op_id1, read_buffer3));
    ObSSTableInfoList read_ss_list;
    pos = 0;
    ObFileBaseObj file_base_obj;
    int64_t len = file_base_obj.get_serialize_size();
    ASSERT_EQ(OB_SUCCESS, read_ss_list.deserialize(read_buffer3.buf_+len, read_buffer3.size_-len, pos));
    LOG_INFO("debug log111", K(write_ss_list), K(read_ss_list));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }

  {
    // test assume and generate sstablelist.op_id that sstable.op_id's finish is true
    // 1. finish op
    CREATE_SSTABLE_LIST_ADD_OP(op_handle);
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
    GET_ONE_SSTABLE_INFO(write_info1, op_id);
    // write sstable task obj and sstable list obj
    write_ss_list.push_back(write_info1);
    write_ss_list.op_id_ = tablet_cur_op_id+1;
    write_ss_list.op_type_ = ObAtomicOpType::SS_LIST_ADD_OP;
    GENERATE_ADD_SSTABEL_TASK_INFO(task_info1, write_info1)
    op_handle.get_atomic_op()->task_base_.finish_upload_ = true;
    ASSERT_EQ(OB_SUCCESS, op_handle.get_atomic_op()->write_add_task_info(task_info1));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->abort_op(op_handle));

    // recheck
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->on_sswriter_resume());
    tablet_cur_op_id += 1;
    // test get current op id obj
    uint64_t cur_op_id = 0;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_current_op_id_(cur_op_id));
    ASSERT_EQ(tablet_cur_op_id, cur_op_id);
    // test read file content with op id
    ObAtomicFileBuffer read_buffer3(read_buf, sizeof(read_buf));
    // read sstable list
    ObSSTableInfoList read_ss_list;
    LOG_INFO("debug log111", K(write_ss_list), K(read_ss_list));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    LOG_INFO("debug log222", K(write_ss_list), K(read_ss_list));
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }
}

TEST_F(TestAtomicFile, test_finish_op_and_read_opt)
{
  MTL(ObTenantDagScheduler*)->stop();
  int ret = OB_SUCCESS;
  char write_buf[MAX_BUF_SIZE];
  char read_buf[MAX_BUF_SIZE];
  GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1, 200002, 1)
  ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, sstablelist_file);
  {
    // finish op
    DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file, 1)
    ASSERT_EQ(tablet_cur_op_id, sstablelist_file->finish_op_id_);
    // read sstable list
    ObSSTableInfoList read_ss_list;
    OB_MOCK_OBJ_SRORAGE.read_cnt_ = 0;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    ASSERT_EQ(1, OB_MOCK_OBJ_SRORAGE.read_cnt_);
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }

  {
    // abort op
    ABORT_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file, 1)
    ASSERT_EQ(tablet_cur_op_id-1, sstablelist_file->finish_op_id_);
    // read sstable list
    ObSSTableInfoList read_ss_list;
    OB_MOCK_OBJ_SRORAGE.read_cnt_ = 0;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    ASSERT_EQ(1, OB_MOCK_OBJ_SRORAGE.read_cnt_);
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }

  {
    mock_switch_sswriter();
    ObSSTableInfoList read_ss_list;
    OB_MOCK_OBJ_SRORAGE.read_cnt_ = 0;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_sstable_list(read_ss_list));
    ASSERT_EQ(5, OB_MOCK_OBJ_SRORAGE.read_cnt_);
    ASSERT_EQ(true, is_array_same(write_ss_list, read_ss_list));
  }

  {
    // do sswriter resume after unknown op state
    tablet_cur_op_id += 1;
    DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file, 1)
  }
}

TEST_F(TestAtomicFile, test_current_obj_not_exsit)
{
  MTL(ObTenantDagScheduler*)->stop();
  int ret = OB_SUCCESS;
  char write_buf[MAX_BUF_SIZE];
  char read_buf[MAX_BUF_SIZE];
  GET_MINI_SSTABLE_LIST_HANDLE(file_handle, 1, 200002, 1)
  ObAtomicSSTableListFile *sstablelist_file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, sstablelist_file);
  {
    ObCurrentOpIdObj current_op_id_obj;
    ObStorageObjectOpt opt;
    MacroBlockId block_id;
    ObStorageObjectType object_type;
    std::cout << "tablet cur op id:" << tablet_cur_op_id << std::endl;
    ASSERT_EQ(OB_SUCCESS, ObAtomicTypeHelper::generate_op_id_obj_opt(ObAtomicFileType::MINI_SSTABLE_LIST,
                                                                     1,
                                                                     200002,
                                                                     10,
                                                                     false,
                                                                     opt));
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, block_id));
    ASSERT_EQ(OB_SUCCESS, MTL(storage::ObTenantFileManager*)->delete_remote_file(block_id));
    ASSERT_EQ(OB_SUCCESS, ObAtomicTypeHelper::generate_op_id_obj_opt(ObAtomicFileType::MINI_SSTABLE_LIST,
                                                                     1,
                                                                     200002,
                                                                     11,
                                                                     false,
                                                                     opt));
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, block_id));
    ASSERT_EQ(OB_SUCCESS, MTL(storage::ObTenantFileManager*)->delete_remote_file(block_id));
    ASSERT_EQ(OB_SUCCESS, ObAtomicTypeHelper::generate_op_id_obj_opt(ObAtomicFileType::MINI_SSTABLE_LIST,
                                                                     1,
                                                                     200002,
                                                                     12,
                                                                     false,
                                                                     opt));
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, block_id));
    ASSERT_EQ(OB_SUCCESS, MTL(storage::ObTenantFileManager*)->delete_remote_file(block_id));

    ObOpIdObj op_id_obj;
    ASSERT_EQ(OB_OBJECT_NOT_EXIST, sstablelist_file->get_op_id_obj_(10, op_id_obj));
    ASSERT_EQ(OB_OBJECT_NOT_EXIST, sstablelist_file->get_op_id_obj_(11, op_id_obj));
    ASSERT_EQ(OB_OBJECT_NOT_EXIST, sstablelist_file->get_op_id_obj_(12, op_id_obj));

    uint64_t cur_op_id = 0;
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->update_current_op_id_(10));
    ASSERT_EQ(OB_SUCCESS, sstablelist_file->get_current_op_id_(cur_op_id));
    ASSERT_EQ(10, cur_op_id);

    DO_ONE_SSTABLE_LIST_ADD_OP(sstablelist_file, 1)
    uint64_t op_id = 0;
    ASSERT_EQ(OB_SUCCESS, op_handle1.get_op_id(op_id));
    ASSERT_EQ(tablet_cur_op_id, op_id);
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_atomic_file.log*");
  OB_LOGGER.set_file_name("test_atomic_file.log", true);
  OB_LOGGER.set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
