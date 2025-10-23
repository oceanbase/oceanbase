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
#include "storage/incremental/atomic_protocol/ob_atomic_file_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_default_file.h"
#include "storage/incremental/atomic_protocol/ob_atomic_sstablelist_op.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "test_ss_atomic_util.h"
#include "shared_storage/clean_residual_data.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
typedef ObAtomicDefaultFile<ObSSLSMetaSSLogValue> ObAtomicLSMetaFile;

static int64_t lease_epoch = 1;

bool is_file_use_sslog(const ObAtomicFileType type, const ObLSID &ls_id)
{
  return false;
}

bool is_meta_use_sslog(const sslog::ObSSLogMetaType type, const ObLSID &ls_id)
{
  return false;
}

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
  is_sswriter = true;
  epoch = ATOMIC_LOAD(&lease_epoch);
  return OB_SUCCESS;
}

class TestAtomicFileMgr : public ::testing::Test
{
public:
  TestAtomicFileMgr() = default;
  virtual ~TestAtomicFileMgr() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

const static int ARRAY_CNT = 5;

void TestAtomicFileMgr::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestAtomicFileMgr::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}


int ObAtomicFileMgr::AtomicFileGCTask::collect_all_ls_tablet_ids_(ObLSTabletMap &ls_tablets_map)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < ARRAY_CNT; i++) {
    common::ObTabletIDArray tablet_ids;
    tablet_ids.push_back(ObTabletID(200001));
    tablet_ids.push_back(ObTabletID(200002));
    tablet_ids.push_back(ObTabletID(200003));
    if (OB_FAIL(ls_tablets_map.set_refactored(i+1, tablet_ids))) {
      STORAGE_LOG(WARN, "failed to set hash set", KR(ret), K(tablet_ids));
    }
  }
  return ret;
}

TEST_F(TestAtomicFileMgr, test_get_sstable_list_handle)
{
  ASSERT_EQ(true, OB_MAX_ATOMIC_FILE_SIZE >= sizeof(ObAtomicFile));
  ASSERT_EQ(true, OB_MAX_ATOMIC_FILE_SIZE >= sizeof(ObAtomicTabletMetaFile));
  ASSERT_EQ(true, OB_MAX_ATOMIC_FILE_SIZE >= sizeof(ObAtomicLSMetaFile));
  int ret = OB_SUCCESS;
  {
    // test normal workflow
    GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1);
    GET_MINI_SSTABLE_LIST_HANDLE_V2(file_handle, 1010, 200001, SCN::base_scn(), 2);
    ASSERT_EQ(true, file_handle1.get_atomic_file() != file_handle2.get_atomic_file());
    ObAtomicFile *atomic_file = NULL;
    ObAtomicFileKey file_key(ObAtomicFileType::MINI_SSTABLE_LIST, ls_id1, tablet_id1);
    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key, atomic_file));
    MTL(ObAtomicFileMgr*)->atomic_file_map_.revert(atomic_file);
    file_handle1.reset();

  }

  // {
  //   // test atomic file not allow concurrent create
  //   ObAtomicFileType type = ObAtomicFileType::MINI_SSTABLE_LIST;
  //   GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 1);
  //   ObAtomicFileKey file_key(type, ls_id1, tablet_id1);
  //   GET_MINI_SSTABLE_LIST_HANDLE_DEFAULT(file_handle, 2);
  //   ObAtomicFile *file_ptr1 = file_handle1.get_atomic_file();

  //   ObAtomicOpHandle<ObAtomicSSTableListAddOp> op_handle;
  //   ASSERT_EQ(OB_SUCCESS, file_ptr1->create_op(op_handle, true, ObString()));

  //   ObAtomicOpHandle<ObAtomicSSTableListAddOp> op_handle2;
  //   ASSERT_EQ(OB_OP_NOT_ALLOW, file_handle2.get_atomic_file()->create_op(op_handle2, true, ObString()));
  //   ASSERT_EQ(OB_SUCCESS, file_ptr1->abort_op(op_handle));
  //   ASSERT_EQ(OB_SUCCESS, file_handle2.get_atomic_file()->create_op(op_handle2, true, ObString()));
  //   ASSERT_EQ(OB_SUCCESS, file_ptr1->abort_op(op_handle2));
  //   file_handle1.get_atomic_file()->set_deleting();
  //   ASSERT_EQ(OB_OP_NOT_ALLOW, file_handle2.get_atomic_file()->create_op(op_handle2, true, ObString()));
  //   // can not use this file to do op because it's deleting
  //   file_handle1.get_atomic_file()->state_.reset();
  //   file_handle1.reset();
  //   file_handle2.reset();
  // }
}

TEST_F(TestAtomicFileMgr, test_get_ls_meta_handle)
{
  int ret = OB_SUCCESS;
  {
    // ls meta
    GET_LS_META_HANLDE_DEFAULT(ls_meta_handle, 1);
    ObAtomicFileType type2 = ObAtomicFileType::LS_META;
    ObAtomicFile *atomic_file = NULL;
    ObAtomicFileKey file_key(type2, ls_id1);
    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key, atomic_file));
    MTL(ObAtomicFileMgr*)->atomic_file_map_.revert(atomic_file);
    ls_meta_handle1.reset();
  }
}

TEST_F(TestAtomicFileMgr, test_atomic_file_mgr_gc)
{
  int ret = OB_SUCCESS;
  {
    // case 1. not gc ls/tablet that has not been deleted and has reference on it
    GET_MINI_SSTABLE_LIST_HANDLE(sstable_handle, 1, 200001, 1);
    GET_LS_META_HANLDE(ls_meta_handle, 1, 2);
    // case 2. not gc ls/tablet that has not been deleted and no reference on it
    GET_MINI_SSTABLE_LIST_HANDLE(sstable_handle, 1, 200002, 3);
    sstable_handle3.reset();
    GET_LS_META_HANLDE(ls_meta_handle, 4, 4);
    ls_meta_handle4.reset();
    // case 3. not gc ls/tablet that has been delete but remains reference on it
    GET_MINI_SSTABLE_LIST_HANDLE(sstable_handle, 1, 200004, 5);
    GET_LS_META_HANLDE(ls_meta_handle, 6, 6);
    // case 4. gc ls/tablet that has been deleted and no reference
    GET_MINI_SSTABLE_LIST_HANDLE(sstable_handle, 1, 200005, 7);
    sstable_handle7.reset();
    GET_LS_META_HANLDE(ls_meta_handle, 8, 8);
    ls_meta_handle8.reset();

    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->gc_task_.process_());

    ObAtomicFileType type = ObAtomicFileType::MINI_SSTABLE_LIST;
    ObAtomicFileKey file_key1(type, ls_id1, tablet_id1);
    ObAtomicFile *atomic_file1 = NULL;
    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key1, atomic_file1));
    ObAtomicFileType type2 = ObAtomicFileType::LS_META;
    ObAtomicFileKey file_key2(type2, ls_id2);
    ObAtomicFile *atomic_file2 = NULL;
    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key2, atomic_file2));

    ObAtomicFileKey file_key3(type, ls_id3, tablet_id3);
    ObAtomicFile *atomic_file3 = NULL;
    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key3, atomic_file3));
    ObAtomicFileKey file_key4(type2, ls_id4);
    ObAtomicFile *atomic_file4 = NULL;
    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key4, atomic_file4));

    ObAtomicFileKey file_key5(type, ls_id5, tablet_id5);
    ObAtomicFile *atomic_file5 = NULL;
    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key5, atomic_file5));
    ObAtomicFileKey file_key6(type2, ls_id6);
    ObAtomicFile *atomic_file6 = NULL;
    ASSERT_EQ(OB_SUCCESS, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key6, atomic_file6));

    ObAtomicFileKey file_key7(type, ls_id7, tablet_id7);
    ObAtomicFile *atomic_file7 = NULL;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key7, atomic_file7));
    ObAtomicFileKey file_key8(type2, ls_id8);
    ObAtomicFile *atomic_file8 = NULL;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObAtomicFileMgr*)->atomic_file_map_.get(file_key8, atomic_file8));
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_atomic_file_mgr.log*");
  OB_LOGGER.set_file_name("test_atomic_file_mgr.log", true);
  OB_LOGGER.set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
