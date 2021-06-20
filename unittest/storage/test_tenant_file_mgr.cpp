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
#include "lib/container/ob_array.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/ob_tenant_mgr.h"
#include "blocksstable/ob_row_generate.h"
#include "blocksstable/ob_data_file_prepare.h"
#define private public
#include "storage/ob_tenant_file_mgr.h"
#undef private

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {

class TestTenantFileMgr : public TestDataFilePrepare {
public:
  TestTenantFileMgr();
  virtual ~TestTenantFileMgr() = default;
  virtual void SetUp();
  virtual void TearDown();

protected:
  void test_pg_meta_checkpoint(const int64_t meta_size);

protected:
  static const int64_t MACRO_BLOCK_SIZE = 128 * 1024;
  common::ObPGKey pg_key_;
};

TestTenantFileMgr::TestTenantFileMgr() : TestDataFilePrepare("tenant_file_mgr", MACRO_BLOCK_SIZE)
{}

void TestTenantFileMgr::SetUp()
{
  TestDataFilePrepare::SetUp();
}

void TestTenantFileMgr::TearDown()
{
  TestDataFilePrepare::TearDown();
}

TEST_F(TestTenantFileMgr, test_file_op)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = 1;
  ObStorageFileHandle handle1;
  ObStorageFileHandle handle2;
  ObStorageFileHandle handle;
  const bool is_sys_table = false;
  ObArray<ObTenantFileInfo*> tenant_file_infos;
  common::ObArenaAllocator allocator;
  int part_id = 0;
  ObPGKey pg_key1(combine_id(1, 3001), ++part_id, 0);
  ObPGKey pg_key2(combine_id(1, 3001), ++part_id, 0);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.alloc_file(tenant_id, is_sys_table, handle1));
  ASSERT_NE(nullptr, handle1.get_storage_file());
  ASSERT_EQ(OB_SUCCESS,
      OB_SERVER_FILE_MGR.add_pg(
          ObTenantFileKey(handle1.get_storage_file()->get_tenant_id(), handle1.get_storage_file()->get_file_id()),
          pg_key1));
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.alloc_file(tenant_id, is_sys_table, handle2));
  ASSERT_NE(nullptr, handle2.get_storage_file());
  ASSERT_EQ(OB_SUCCESS,
      OB_SERVER_FILE_MGR.add_pg(
          ObTenantFileKey(handle2.get_storage_file()->get_tenant_id(), handle2.get_storage_file()->get_file_id()),
          pg_key2));
  ASSERT_NE(handle1.get_storage_file(), handle2.get_storage_file());
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_all_tenant_file_infos(allocator, tenant_file_infos));
  ASSERT_EQ(2, tenant_file_infos.count());

  // alloc less than 10000 files
  for (int64_t i = 0; OB_SUCC(ret) && i < 9998; ++i) {
    ObPGKey pg_key(combine_id(1, 3001), ++part_id, 0);
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.alloc_file(tenant_id, is_sys_table, handle));
    ASSERT_NE(nullptr, handle.get_storage_file());
    ASSERT_EQ(OB_SUCCESS,
        OB_SERVER_FILE_MGR.add_pg(
            ObTenantFileKey(handle.get_storage_file()->get_tenant_id(), handle.get_storage_file()->get_file_id()),
            pg_key));
  }
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_all_tenant_file_infos(allocator, tenant_file_infos));
  ASSERT_EQ(10, tenant_file_infos.count());

  // alloc more than 10000 files
  for (int64_t i = 0; OB_SUCC(ret) && i < 3; ++i) {
    ObPGKey pg_key(combine_id(1, 3001), ++part_id, 0);
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.alloc_file(tenant_id, is_sys_table, handle));
    ASSERT_NE(nullptr, handle.get_storage_file());
    ASSERT_EQ(OB_SUCCESS,
        OB_SERVER_FILE_MGR.add_pg(
            ObTenantFileKey(handle.get_storage_file()->get_tenant_id(), handle.get_storage_file()->get_file_id()),
            pg_key));
  }
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_all_tenant_file_infos(allocator, tenant_file_infos));
  ASSERT_EQ(11, tenant_file_infos.count());
}

TEST_F(TestTenantFileMgr, test_update_tenant_file_super_block)
{
  int ret = OB_SUCCESS;
  ObStorageFileHandle handle1;
  ObStorageFileHandle handle2;
  const bool is_sys_table = false;
  const int64_t tenant_id = 1;
  ObTenantFileKey tenant_key;
  ObArray<ObTenantFileInfo*> tenant_file_infos;
  tenant_key.tenant_id_ = tenant_id;
  ObTenantFileSuperBlock new_tenant_super_block;
  common::ObArenaAllocator allocator;
  new_tenant_super_block.macro_meta_entry_.macro_block_id_.set_local_block_id(1);
  new_tenant_super_block.pg_meta_entry_.macro_block_id_.set_local_block_id(2);
  new_tenant_super_block.status_ = TENANT_FILE_NORMAL;
  ObTenantFileCheckpointEntry file_entry;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.alloc_file(tenant_id, is_sys_table, handle1));
  ASSERT_NE(nullptr, handle1.get_storage_file());
  tenant_key.file_id_ = handle1.get_storage_file()->get_file_id();
  file_entry.tenant_file_key_ = tenant_key;
  file_entry.super_block_ = new_tenant_super_block;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_tenant_file(tenant_key, handle2));
  ASSERT_EQ(handle2.get_storage_file(), handle1.get_storage_file());
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.update_tenant_file_meta_info(file_entry, true));
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_all_tenant_file_infos(allocator, tenant_file_infos));
  int64_t i = 0;
  for (i = 0; OB_SUCC(ret) && i < tenant_file_infos.count(); ++i) {
    ObTenantFileInfo& file_info = *tenant_file_infos.at(i);
    if (file_info.tenant_key_ == tenant_key) {
      ASSERT_EQ(file_info.tenant_file_super_block_.macro_meta_entry_.macro_block_id_,
          new_tenant_super_block.macro_meta_entry_.macro_block_id_);
      ASSERT_EQ(file_info.tenant_file_super_block_.pg_meta_entry_.macro_block_id_,
          new_tenant_super_block.pg_meta_entry_.macro_block_id_);
      break;
    }
  }
  ASSERT_NE(i, tenant_file_infos.count());
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_tenant_file_mgr.log*");
  OB_LOGGER.set_file_name("test_tenant_file_mgr.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_tenant_file_mgr");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
