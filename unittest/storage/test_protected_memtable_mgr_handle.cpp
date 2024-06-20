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

#include "share/ob_errno.h"
#include <gtest/gtest.h>
#include "storage/test_tablet_helper.h"
#include <lib/oblog/ob_log.h>
#include "storage/tx_storage/ob_ls_service.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "init_basic_struct.h"

#define USING_LOG_PREFIX STORAGE

#define UNITTEST

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{
class TestTabletMemtableMgr : public ::testing::Test
{
public:
  TestTabletMemtableMgr() {}
  virtual ~TestTabletMemtableMgr() = default;

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
};

TEST_F(TestTabletMemtableMgr, tablet_memtable_mgr) {
  ObArenaAllocator allocator;
  ObLSID ls_id(2000);
  ObTabletID tablet_id(500000);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObTabletMemtableMgrPool *pool = MTL(ObTabletMemtableMgrPool*);
  ObCreateLSArg arg;
  ObTabletHandle tablet_handle;
  share::schema::ObTableSchema table_schema;
  share::SCN scn1;
  scn1.set_base();
  share::SCN scn2 = share::SCN::scn_inc(scn1);
  uint64_t table_id = 12345;
  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  ObTableHandleV2 handle;

  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(1, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(true, tablet_handle.is_valid());
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_protected_memtable_mgr_handle(protected_handle));

  // memtable_mgr not exist
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, protected_handle->get_active_memtable(handle));
  ASSERT_EQ(0, pool->count_);

  // create memtable
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->create_memtable(1, scn1, false, false));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->create_memtable(2, scn2, false, false));

  ObSEArray<ObTableHandleV2, 64> handles;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_all_memtables(handles));

  // memtable_mgr exist
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_active_memtable(handle));
  ASSERT_EQ(1, pool->count_);

  // release a memtable, memtable count is 1
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->release_memtables(scn1));

  // memtable_mgr exist yet
  ASSERT_EQ(OB_SUCCESS, protected_handle->get_active_memtable(handle));
  ASSERT_EQ(1, pool->count_);

  // release other memtable, memtable count is 0, so release tablet_mgr
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->release_memtables());

  // memtable_mgr not exist
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, protected_handle->get_active_memtable(handle));
  // memtable mgr is not destroyed so memtable also hold an reference
  ASSERT_EQ(1, pool->count_);

  // remove memtable mgr reference from memtable
  ASSERT_EQ(1, handles.count());
  for (int i = 0; i < handles.count(); i++) {
    ObIMemtable *i_mt = nullptr;
    EXPECT_EQ(OB_SUCCESS, handles[i].get_memtable(i_mt));
    memtable::ObMemtable *mt = (memtable::ObMemtable *)(i_mt);
    mt->memtable_mgr_handle_.reset();
  }
  ASSERT_EQ(0, pool->count_);

  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id));
}


}
} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f test_checkpoint_diagnose.log*");
  OB_LOGGER.set_file_name("test_tablet_memtable_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  signal(49, SIG_IGN);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
